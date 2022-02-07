import os
from grpc_tools import command

command.build_package_protos('.')

init_files_to_create = []

for root, dirs, files in os.walk('kikimr'):
    if '__init__.py' in files:
        continue

    init_files_to_create.append(
        os.path.join(
            root,
            '__init__.py'
        )
    )


for filename in init_files_to_create:
    with open(filename, 'w') as f:
        pass
