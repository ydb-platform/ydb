import datetime
import os
import requests
import subprocess
import sys

import yatest.common
from library.python.testing.recipe import declare_recipe
from library.recipes.common import start_daemon, stop_daemon
from yatest.common import network

TVMAPI_PORT_FILE = "tvmapi.port"
TVMAPI_PID_FILE = "tvmapi.pid"

TVMCERT_PORT_FILE = "tvmcert.port"

CONFIG_PATH = './tvm-api.config.xml'


def test_data_path():
    return yatest.common.source_path() + '/library/recipes/tvmapi/data/'


PORT_MANAGER = network.PortManager()


def _gen_config(cfg_template):
    http_port = PORT_MANAGER.get_tcp_port(80)
    tvmcert_port = PORT_MANAGER.get_tcp_port(9001)

    f = open(cfg_template)
    cfg = f.read()

    cfg = cfg.replace('{port}', str(http_port))

    cfg = cfg.replace('{secret.key}', test_data_path() + 'secret.key')
    cfg = cfg.replace('{test_secret.key}', test_data_path() + 'test_secret.key')

    cfg = cfg.replace('{tvmdb_credentials}', test_data_path() + 'tvmdb.credentials')
    cfg = cfg.replace('{client_secret}', test_data_path() + 'client_secret.secret')
    cfg = cfg.replace('{tvm_cache}', test_data_path() + "tvm_cache")

    cfg = cfg.replace('{abc.json}', test_data_path() + 'abc.json')
    cfg = cfg.replace('{staff.json}', test_data_path() + 'staff.json')

    cfg = cfg.replace('{tvmcert_port}', str(tvmcert_port))

    print(cfg, file=sys.stderr)

    f = open(CONFIG_PATH, 'wt')
    f.write(cfg)

    return http_port, tvmcert_port


def _prepare_db(sql, db):
    SQLITE_BIN = yatest.common.build_path() + '/contrib/tools/sqlite3/sqlite3'
    if os.path.isfile(db):
        os.remove(db)

    input_sql = open(sql)
    p = subprocess.run([SQLITE_BIN, db], stdin=input_sql)
    assert 0 == p.returncode


def start(argv):
    _log('Starting TVM recipe')

    def pop_arg(def_val):
        if len(argv) > 0:
            return yatest.common.source_path(argv.pop(0))
        return test_data_path() + def_val

    dbfile = pop_arg('tvm.sql')
    cfg_template = pop_arg('config.xml')

    _prepare_db(dbfile, './tvm.db')

    http_port, tvmcert_port = _gen_config(cfg_template)

    print(http_port, tvmcert_port, file=sys.stderr)
    with open(TVMAPI_PORT_FILE, "w") as f:
        f.write(str(http_port))

    with open(TVMCERT_PORT_FILE, "w") as f:
        f.write(str(tvmcert_port))

    # launch tvm
    args = [
        yatest.common.build_path() + '/passport/infra/daemons/tvmapi/daemon/tvm',
        '-c',
        CONFIG_PATH,
    ]

    def check():
        try:
            r = requests.get("http://localhost:%d/nagios" % http_port)
            if r.status_code == 200:
                return True
            else:
                _log("ping: %d : %s" % (r.status_code, r.text))
        except Exception as e:
            _log("ping: %s" % e)
        return False

    start_daemon(command=args, environment=os.environ.copy(), is_alive_check=check, pid_file_name=TVMAPI_PID_FILE)


def stop(argv):
    with open(TVMAPI_PID_FILE) as f:
        pid = f.read()
    if not stop_daemon(pid):
        _log("pid is dead: %s" % pid)


def _log(msg):
    print("%s : tvmapi-recipe : %s" % (datetime.datetime.now(), msg), file=sys.stdout)


if __name__ == "__main__":
    declare_recipe(start, stop)
