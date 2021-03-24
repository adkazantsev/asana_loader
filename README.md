# Example usage:

asl = AsanaLoader(token = '1/777777777777777:abcdefghijklmnopqrstuvwxyz')

# print projects inside
for workspace, projects in asl.get_projects_in_workspace().items():
    for project in projects:
        print(project)

# Download all projects
for workspace in asl.get_workspaces():
    print(f'Start downloading {workspace=}.')
    asl.download_workspace(workspace, target_folder='.')