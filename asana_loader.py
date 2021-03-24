import json
import asana
import os

from glob import glob, escape
import urllib.request


class AsanaLoader:

    def __init__(self, token):
        self.token = token
        self.client = asana.Client.access_token(token)

    def get_workspaces(self):
        return list(self.client.workspaces.get_workspaces())

    def get_workspace_by_id(self, gid):
        return self.client.workspaces.get_workspace(gid)

    def get_workspace_by_name(self, name):
        for workspace in self.get_workspaces():
            if workspace.name == name:
                return workspace
        raise Exception(f'Cannot find workspace with the {name=}')

    def get_projects_in_workspace(self, gid=None):
        if gid is None:
            res = {}
            for workspace in self.get_workspaces():
                res[workspace['gid']] = list(self.client.projects.get_projects(workspace=workspace['gid']))
            return res
        else:
            return list(self.client.projects.get_projects(workspace=gid))

    def get_or_create_project_in_workspace_by_name(self, workspace_gid, name):
        for project in self.get_projects_in_workspace(workspace_gid):
            if project['name'] == name:
                return project
        res = self.client.projects.create_in_workspace(workspace_gid, {'name': name})
        return res

    def get_or_create_task_in_project(self, project_gid, task, verbose=False):
        for task_ in self.client.tasks.get_tasks(project=project_gid):
            if task_['name'] == task['name']:
                if verbose:
                    print(f'Found task "{task["name"]}" already uploaded')
                return task_
        task['projects'] = [project_gid]
        if verbose:
            print(f'Creating task "{task["name"]}"')
        res = self.client.tasks.create(task)
        return res

    def create_task_in_project(self, project_gid, task, verbose=False):
        task['projects'] = [project_gid]
        if verbose:
            print(f'[create_task_in_project] Creating task "{task["name"]}"')
        res = self.client.tasks.create(task)
        return res

    def create_story_for_task(self, task_gid, story, verbose=False):
        story_res = self.client.stories.create_story_for_task(task_gid, story)
        if verbose:
            print(f'[create_story_for_task] Created story: {story} for {task_gid=}')
        return story_res

    def download_project(self, gid, target_folder, num_threads=None, verbose=False, raise_if_exists=False):
        tasks = self.client.tasks.get_tasks(project=gid)
        if num_threads is not None:
            raise Exception(f'Implement!')
            # with Pool(PRC_COUNT) as pool:
            #    pool.map(download_task_, ((task, target_folder) for task in tasks))
        else:
            for task in tasks:  # note that task here has only fields: 'gid', 'name', 'resource_type'
                self.download_task(task, target_folder, verbose=verbose, raise_if_exists=raise_if_exists)

    def delete_tasks_in_project(self, project_gid, verbose=False):
        for task in self.client.tasks.get_tasks_for_project(project_gid):
            if verbose:
                print(f'[delete_tasks_in_project] Deleting task "{task["name"]}"')
            self.client.tasks.delete(task['gid'])

    def dump_item(self, gid, item_type, path, verbose=False, raise_if_exists=False):
        if os.path.isfile(path):
            if verbose:
                print(f'Already saved: {path} {gid=} {item_type=}')
            if raise_if_exists:
                raise Exception(f'File {path} already dumped!')
            return
        with open(path, 'w', encoding='utf-8') as f:
            if verbose:
                print(f'[dump_item] Dumping item with {gid=} to "{path}"')
            if item_type == 'task':
                res = self.client.tasks.get_task(gid)
            elif item_type == 'stories':
                res = list(self.client.stories.get_stories_for_task(gid))
            elif item_type == 'attachments':
                res = list(self.client.attachments.get_attachments_for_task(gid))
            elif item_type == 'subtasks':
                res = list(self.client.tasks.get_subtasks_for_task(gid))
            else:
                raise Exception(f'Unknown item_type: {item_type}')

            if len(res) == 100:
                print(f'WARNING! Received 100 responses! Maybe we should fix this (limitation?)')

            json.dump(res, f, ensure_ascii=False, indent=4)

    def read_item(self, path, verbose=False):
        assert os.path.isfile(path)
        with open(path) as f:
            return json.load(f)

    def download_attachment(self, attachment_gid, download_folder, verbose=False):
        search_path = escape(f'{download_folder}/{attachment_gid}')
        if len(glob(f'{search_path}_*')) > 0:
            if verbose:
                print(f'[download_attachment] Attachment "{attachment_gid}" already downloaded.')
            return

        attachment = self.client.attachments.get_attachment(attachment_gid)
        assert attachment['gid'] == attachment_gid
        url, attachment_name = attachment['download_url'], attachment['name']

        fname = f'{download_folder}/{attachment_gid}_{attachment_name}'
        assert not os.path.isfile(fname), f'{attachment_gid=} {download_folder=}'
        try:
            urllib.request.urlretrieve(url, fname)
        except TypeError:
            print(f'Warning! cannot download {attachment}')

    def download_task(self, task, target_folder, depth=0, verbose=True, raise_if_exists=False):
        if depth > 4:
            print(f'WARNING! Too deep task, wont copy (circular reference?)')
            return

        task_gid = task['gid']
        name     = task_gid + '_' + task['name'].replace('/', '|')[:60]

        os.makedirs(target_folder, exist_ok=True)
        os.makedirs(f'{target_folder}/{name}', exist_ok=True)

        self.dump_item(task_gid, 'task',        f'{target_folder}/{name}/task.json',        verbose=verbose, raise_if_exists=raise_if_exists)
        self.dump_item(task_gid, 'stories',     f'{target_folder}/{name}/stories.json',     verbose=verbose, raise_if_exists=raise_if_exists)
        self.dump_item(task_gid, 'attachments', f'{target_folder}/{name}/attachments.json', verbose=verbose, raise_if_exists=raise_if_exists)
        self.dump_item(task_gid, 'subtasks',    f'{target_folder}/{name}/subtasks.json',    verbose=verbose, raise_if_exists=raise_if_exists)

        for attachment in self.read_item(f'{target_folder}/{name}/attachments.json'):
            self.download_attachment(attachment['gid'], download_folder=f'{target_folder}/{name}')

        for subtask in self.read_item(f'{target_folder}/{name}/subtasks.json'):
            if verbose:
                print(f'[download_task] Downloading subtask to {target_folder}/{name}. {subtask=}')

            self.download_task(subtask, f'{target_folder}/{name}', depth=depth + 1, raise_if_exists=raise_if_exists)

    def download_workspace(self, workspace, target_folder, only_project_gids=None):
        for project in self.get_projects_in_workspace(workspace['gid']):
            if only_project_gids and project['gid'] not in only_project_gids:
                continue
            print(f'\nStart downloading {project=}')
            self.download_project(project['gid'], f'{target_folder}/{workspace["gid"]}_{workspace["name"]}/{project["gid"]}_{project["name"]}', verbose=True)

    def _upload_task(self, task_path, project_gid, verbose=False):
        with open(f'{task_path}/task.json') as f:
            task = json.load(f)

            for field in {'gid', 'start_on', 'assignee', 'assignee_status', 'completed_at', 'created_at', 'followers', 'hearts', 'liked', 'likes', 'memberships',
                          'modified_at', 'num_hearts', 'num_likes', 'permalink_url', 'projects', 'resource_type', 'section', 'workspace', 'parent', 'tags'}:
                if field in task:
                    del task[field]
            if ('due_at' in task) and ('due_on' in task):
                del task['due_on']
        task = self.create_task_in_project(project_gid, task, verbose=verbose)
        return task

    def _upload_subtask(self, subtask_path, task_gid, verbose=False):
        with open(f'{subtask_path}/task.json') as f:
            subtask = json.load(f)
            for field in {'gid', 'start_on', 'assignee', 'assignee_status', 'completed_at', 'created_at', 'followers', 'hearts', 'liked', 'likes', 'memberships',
                          'modified_at', 'num_hearts', 'num_likes', 'permalink_url', 'projects', 'resource_type', 'section', 'workspace', 'parent', 'tags'}:
                if field in subtask:
                    del subtask[field]

        if verbose:
            print(f'[_upload_subtask] Uploading subtask: {subtask_path}')
        subtask = self.client.tasks.create_subtask_for_task(task_gid, subtask)
        return subtask

    def _upload_stories(self, task_path, task_gid, verbose=False):
        # Upload stories
        with open(f'{task_path}/stories.json') as f:
            stories = json.load(f)
            res = []
            for story in stories:
                if (story['type'] == 'system') or ('text' not in story):
                    continue

                story['text'] = '[' + story["created_at"] + ', ' + story["created_by"]["name"] + '] ' + story['text']

                for field in {'resource_type', 'gid', 'created_at', 'created_by', 'type', 'resource_subtype'}:
                    if field in story:
                        del story[field]

                story = self.create_story_for_task(task_gid, story, verbose=verbose)
                res.append(story)
        return res

    def _upload_attachments(self, task_path, task_gid, verbose=False):
        res = []
        search_path = escape(task_path)
        for path in glob(f'{search_path}/*'):
            if (not os.path.isfile(path)) or (os.path.basename(path) in {'stories.json', 'task.json', 'attachments.json', 'subtasks.json'}):
                continue

            if verbose:
                print(f'[_upload_attachments] Processing attachment {path}')

            _, file_name = os.path.basename(path).split('_', 1)

            if os.path.getsize(path) > 90_000_000:
                print(f'WARNING! Attachment {path} exceeds max_size. Will not upload it')
                continue

            with open(path, 'rb') as f:
                attach_respone = self.client.attachments.create_attachment_for_task(task_gid, f, file_name)
                res.append(attach_respone)
        return res

    def upload_task(self, task_path, parent, verbose=False):
        if parent['resource_type'] == 'project':
            task = self._upload_task(task_path, parent['gid'], verbose=verbose)
        elif parent['resource_type'] == 'task':
            task = self._upload_subtask(task_path, parent['gid'], verbose=verbose)
        else:
            raise Exception(f'Unknown parent resource_type: {parent["resource_type"]}')

        self._upload_stories(task_path, task['gid'], verbose=verbose)
        self._upload_attachments(task_path, task['gid'], verbose=verbose)

        # processing subtasks
        search_path = escape(task_path)
        for path in glob(f'{search_path}/*'):
            if (not os.path.isdir(path)):
                continue
            if verbose:
                print(f'[upload_task] Processing subtask: {path}')
            self.upload_task(path, task)

    def upload_project(self, project_path, workspace_gid, team=None, skip_uploaded_projects=True, skip_uploaded_tasks=False, verbose=False):
        _, project_name = os.path.basename(project_path).split('_', 1)
        if verbose:
            print(f'[upload_project] Processing project "{project_name}"')

        uploaded_projects = self.get_projects_in_workspace(gid=workspace_gid)
        for project in uploaded_projects:
            if project['name'] == project_name:
                if skip_uploaded_projects:
                    print(f'WARNING! Project "{project_name}" is already uploaded!')
                    return
                else:
                    print(f'[upload_project] Found project {project=}')
                    break
        else:
            project_ = {'name': project_name}
            if team is not None:
                project_['team'] = team
            project = self.client.projects.create_in_workspace(workspace_gid, project_)
            if verbose:
                print(f'[upload_project] Uploading {project=}')

        uploaded_task_names = list(t['name'] for t in self.client.tasks.get_tasks_for_project(project['gid']))
        search_path = escape(project_path)
        for path in glob(f'{search_path}/*'):
            if not os.path.isdir(path):
                continue

            if skip_uploaded_tasks:
                with open(f'{path}/task.json') as f:
                    task = json.load(f)
                    if task['name'] in uploaded_task_names:
                        print(f'[upload_project] Task {task["name"]} is already uploaded!')
                        continue
            self.upload_task(task_path=path, parent={'resource_type': 'project', 'gid': project['gid']}, verbose=True)
