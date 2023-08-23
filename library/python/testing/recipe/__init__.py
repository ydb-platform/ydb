from __future__ import print_function

import os
import sys
import json
import logging
import argparse

from yatest_lib.ya import Ya

import yatest.common as yc

RECIPE_START_OPTION = "start"
RECIPE_STOP_OPTION = "stop"

ya = None
collect_cores = None
sanitizer_extra_checks = None


class Config:
    def __init__(self):
        self.ya = ya
        self.collect_cores = collect_cores
        self.sanitizer_extra_checks = sanitizer_extra_checks


def _setup_logging(level=logging.DEBUG):
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    log_format = '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s: %(message)s'

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(log_format)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)


def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("--show-cwd", action="store_true", dest="show_cwd", default=False, help="show recipe cwd")
    parser.add_argument("--test-debug", action="store_true", dest="test_debug", default=False, help="test debug mode")
    parser.add_argument("--test-stderr", action="store_true", dest="test_stderr", default=False, help="test stderr")
    parser.add_argument("--pdb", action="store_true", dest="pdb", default=False, help="run pdb on error")
    parser.add_argument("--sanitizer-extra-checks", dest="sanitizer_extra_checks", action="store_true", default=False, help="enables extra checks for tests built with sanitizers")
    parser.add_argument("--collect-cores", dest="collect_cores", action="store_true", default=False, help="allows core dump file recovering during test")
    parser.add_argument("--build-root", type=str, dest="build_root", default=None, help="Build root directory")
    parser.add_argument("--source-root", type=str, dest="source_root", default=None, help="Source root directory")
    parser.add_argument("--output-dir", type=str, dest="output_dir", default=None, help="Output directory")
    parser.add_argument("--env-file", type=str, dest="env_file", default=None, help="File to read/write environment variables")

    args, opts = parser.parse_known_args()

    global ya, sanitizer_extra_checks, collect_cores
    _setup_logging()

    context = {
        "test_stderr": args.test_stderr,
    }

    ya = Ya(context=context, build_root=args.build_root, source_root=args.source_root, output_dir=args.output_dir, env_file=args.env_file)

    ya._data_root = ""  # XXX remove

    sanitizer_extra_checks = args.sanitizer_extra_checks
    if sanitizer_extra_checks:
        for envvar in ['LSAN_OPTIONS', 'ASAN_OPTIONS']:
            if envvar in os.environ:
                os.environ.pop(envvar)
            if envvar + '_ORIGINAL' in os.environ:
                os.environ[envvar] = os.environ[envvar + '_ORIGINAL']
    collect_cores = args.collect_cores

    yc.runtime._set_ya_config(config=Config())
    for recipe_option in RECIPE_START_OPTION, RECIPE_STOP_OPTION:
        if recipe_option in opts:
            return args, opts[opts.index(recipe_option):]


def set_env(key, value):
    with open(ya.env_file, "a") as f:
        json.dump({key: value}, f)
        f.write("\n")


def tty():
    if os.isatty(1):
        return

    f = open('/dev/tty', 'w+')
    fd = f.fileno()
    os.dup2(fd, 0)
    os.dup2(fd, 1)
    os.dup2(fd, 2)


def declare_recipe(start, stop):
    parsed_args, argv = get_options()

    if parsed_args.show_cwd:
        print("Recipe \"{} {}\" working dir is {}".format(sys.argv[0], " ".join(argv), os.getcwd()))

    try:
        if argv[0] == RECIPE_START_OPTION:
            start(argv[1:])
        elif argv[0] == RECIPE_STOP_OPTION:
            stop(argv[1:])
    except Exception:
        if parsed_args.pdb:
            tty()
            import ipdb
            ipdb.post_mortem()
        else:
            raise
