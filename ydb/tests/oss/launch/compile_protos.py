import os
import sys
from argparse import ArgumentParser
from grpc_tools import protoc


def build_ydb_protos(ydb_repo_root, proto_dir='ydb', strict_mode=False):
    proto_files = []
    repo_root = os.path.abspath(ydb_repo_root)
    files_root = os.path.join(repo_root, proto_dir)
    # print("Repo root:", repo_root, file=sys.stderr)
    # print("Proto files root:", files_root, file=sys.stderr)
    for root, _, files in os.walk(files_root):
        for filename in files:
            if filename.endswith('.proto'):
                proto_files.append(os.path.abspath(os.path.join(root,
                                                                filename)))

    include_paths = [
        'contrib/libs/googleapis-common-protos',
        'contrib/libs/protobuf/src',
    ]

    command_fix = [
        'grpc_tools.protoc',
        '--proto_path={}'.format(repo_root),
        '--python_out={}'.format(repo_root),
        '--pyi_out={}'.format(repo_root),
        '--grpc_python_out={}'.format(repo_root),
    ]

    for ipath in include_paths:
        command_fix = command_fix + [
            '--proto_path={}'.format(os.path.join(repo_root, ipath)),
        ]

    for proto_file in proto_files:
        # print(proto_file, file=sys.stderr)
        command = command_fix + [proto_file]
        if protoc.main(command) != 0:
            if strict_mode:
                raise Exception('error: {} failed'.format(command))
            else:
                sys.stderr.write('warning: {} failed'.format(command))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('directory', help='Directory inside YDB repo to scan for .proto files', default='ydb', nargs='+')
    parser.add_argument('--source-root', required=True, help='YDB repo source directory')
    parser.add_argument('--strict-mode', required=False, default=False, help='Strict mode (fail with result code 1 on errors)')
    args = parser.parse_args()
    for dr in args.directory:
        # print("Dir:", dr, file=sys.stderr)
        build_ydb_protos(args.source_root, dr, args.strict_mode)
