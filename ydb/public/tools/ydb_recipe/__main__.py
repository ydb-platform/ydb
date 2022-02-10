# -*- coding: utf-8 -*-
import argparse

from ydb.public.tools.lib import cmds
from library.python.testing.recipe import declare_recipe


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-packages", action="store", default=None)
    parser.add_argument("--suppress-version-check", action='store_true', default=False)
    parser.add_argument("--ydb-working-dir", action="store")
    parser.add_argument("--debug-logging", nargs='*')
    parser.add_argument("--enable-pq", action='store_true', default=False)
    parser.add_argument("--enable-datastreams", action='store_true', default=False)
    parser.add_argument("--enable-pqcd", action='store_true', default=False)
    parsed, _ = parser.parse_known_args()
    arguments = cmds.EmptyArguments()
    arguments.suppress_version_check = parsed.suppress_version_check
    arguments.ydb_working_dir = parsed.ydb_working_dir
    if parsed.use_packages is not None:
        arguments.use_packages = parsed.use_packages
    if parsed.debug_logging:
        arguments.debug_logging = parsed.debug_logging
    arguments.enable_pq = parsed.enable_pq
    arguments.enable_datastreams = parsed.enable_datastreams
    arguments.enable_pqcd = parsed.enable_pqcd
    declare_recipe(
        lambda args: cmds.deploy(arguments),
        lambda args: cmds.cleanup(arguments),
    )
