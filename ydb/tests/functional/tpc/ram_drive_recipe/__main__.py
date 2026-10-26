import json
import logging
import os
import shutil
import tempfile

from library.python.testing.recipe import declare_recipe, set_env
import yatest.common

TMP_ENV = ('TMPDIR', 'TMP', 'TEMP')
STATE_ENV = 'YDB_TPC_RAM_TMP_STATE'


def start(argv):
    ram_drive = yatest.common.ram_drive_path()
    if not ram_drive:
        logging.info('No RAM drive provided; keeping the default temporary directory')
        return

    tmp_dir = tempfile.mkdtemp(prefix='tpc-', dir=ram_drive)
    state = {'tmp_dir': tmp_dir, 'original_env': {name: os.getenv(name) for name in TMP_ENV}}
    set_env(STATE_ENV, json.dumps(state))
    for name in TMP_ENV:
        set_env(name, tmp_dir)
    logging.info('Using RAM-backed temporary directory: %s', tmp_dir)


def stop(argv):
    state_json = os.getenv(STATE_ENV)
    if not state_json:
        return

    state = json.loads(state_json)
    ram_drive = yatest.common.ram_drive_path()
    tmp_dir = os.path.realpath(state['tmp_dir'])
    if (
        not ram_drive
        or os.path.dirname(tmp_dir) != os.path.realpath(ram_drive)
        or not os.path.basename(tmp_dir).startswith('tpc-')
    ):
        raise RuntimeError(f'Unexpected temporary directory for cleanup: {tmp_dir}')

    for name, value in state['original_env'].items():
        set_env(name, value)
    set_env(STATE_ENV, None)
    shutil.rmtree(tmp_dir)


if __name__ == '__main__':
    declare_recipe(start, stop)
