from argparse import ArgumentParser
from grpc_tools import command


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--source-root', required=True, help='YDB source directory')
    args = parser.parse_args()

    command.build_package_protos(args.source_root)
