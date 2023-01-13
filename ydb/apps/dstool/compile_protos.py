#!/usr/bin/env python3

from argparse import ArgumentParser
from grpc_tools import command

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--ydb-root", type=str, required=True, help="YDB root directory")
    args = parser.parse_args()
    command.build_package_protos(args.ydb_root)
