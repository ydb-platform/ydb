"""
# MkDocs Integration tests

This is a simple integration test that builds the MkDocs
documentation against all of the builtin themes.

From the root of the MkDocs git repo, use:

    python -m mkdocs.tests.integration --help


TODOs
    - Build with different configuration options.
    - Build documentation other than just MkDocs as it is relatively simple.
"""


import click
import logging
import os
import subprocess

from mkdocs import utils
from mkdocs.tests.base import DATA_DIR

import yatest.common as yac

log = logging.getLogger('mkdocs')

DIR = yac.work_path()
MKDOCS_CONFIG = os.path.abspath(os.path.join(DIR, 'docs/mkdocs.yml'))
MKDOCS_THEMES = utils.get_theme_names()
TEST_PROJECTS = os.path.abspath(os.path.join(DATA_DIR, 'integration'))


def test_main():
    output = yac.work_path()

    log.propagate = False
    stream = logging.StreamHandler()
    formatter = logging.Formatter(
        "\033[1m\033[1;32m *** %(message)s *** \033[0m")
    stream.setFormatter(formatter)
    log.addHandler(stream)
    log.setLevel(logging.DEBUG)

    base_cmd = [yac.binary_path('contrib/python/mkdocs/bin/mkdocs'), 'build', '-s', '-v', '--site-dir', ]

    log.debug("Building installed themes.")
    for theme in sorted(MKDOCS_THEMES):
        log.debug(f"Building theme: {theme}")
        project_dir = os.path.dirname(MKDOCS_CONFIG)
        out = os.path.join(output, theme)
        command = base_cmd + [out, '--theme', theme]
        subprocess.check_call(command, cwd=project_dir)

    log.debug("Building test projects.")
    for project in os.listdir(TEST_PROJECTS):
        log.debug(f"Building test project: {project}")
        project_dir = os.path.join(TEST_PROJECTS, project)
        out = os.path.join(output, project)
        command = base_cmd + [out, ]
        subprocess.check_call(command, cwd=project_dir)

    log.debug(f"Theme and integration builds are in {output}")
