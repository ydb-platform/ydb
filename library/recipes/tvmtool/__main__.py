import argparse
import datetime
import binascii
import os
import requests
import sys

from library.python.testing.recipe import declare_recipe
from library.recipes.common import start_daemon, stop_daemon
import yatest.common
import yatest.common.network

TVMTOOL_PORT_FILE = "tvmtool.port"
TVMTOOL_AUTHTOKEN_FILE = "tvmtool.authtoken"

TIROLE_PORT_FILE = "tirole.port"
TVMAPI_PORT_FILE = "tvmapi.port"
TVMTOOL_PID_FILE = "tvmtool.pid"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('cfgfile', type=str)
    parser.add_argument('--with-roles-dir', dest='with_roles_dir', type=str)
    parser.add_argument('--with-tirole', dest='with_tirole', action='store_true')
    parser.add_argument('--with-tvmapi', dest='with_tvmapi', action='store_true')
    input_args = parser.parse_args(argv)

    _log("cfgfile: %s" % input_args.cfgfile)
    _log("with-roles-dir: %s" % input_args.with_roles_dir)
    _log("with-tirole: %s" % input_args.with_tirole)
    _log("with-tvmapi: %s" % input_args.with_tvmapi)

    pm = yatest.common.network.PortManager()
    port = pm.get_tcp_port(80)

    with open(TVMTOOL_PORT_FILE, "w") as f:
        f.write(str(port))
    _log("port: %d" % port)

    authtoken = binascii.hexlify(os.urandom(16))
    with open(TVMTOOL_AUTHTOKEN_FILE, "wb") as f:
        f.write(authtoken)
    _log("authtoken: %s" % authtoken)

    args = [
        yatest.common.build_path('passport/infra/daemons/tvmtool/cmd/tvmtool'),
        '--port',
        str(port),
        '-c',
        yatest.common.source_path(input_args.cfgfile),
        '-v',
        '--cache-dir',
        './',
    ]
    env = {
        'QLOUD_TVM_TOKEN': authtoken,
    }

    if input_args.with_tvmapi:
        with open(TVMAPI_PORT_FILE) as f:
            env['__TEST_TVM_API_URL'] = "http://localhost:%s" % f.read()
    else:
        args.append('--unittest')

    if input_args.with_tirole:
        with open(TIROLE_PORT_FILE) as f:
            env['__TEST_TIROLE_URL'] = "http://localhost:%s" % f.read()

    if input_args.with_roles_dir:
        assert not input_args.with_tirole, "--with-roles-dir and --with-tirole conflicts with each other"
        args += [
            '--unittest-roles-dir',
            yatest.common.source_path(input_args.with_roles_dir),
        ]

    def check():
        try:
            r = requests.get("http://localhost:%d/tvm/ping" % port)
            if r.status_code == 200:
                _log("ping: 200!")
                return True
            else:
                _log("ping: %d : %s" % (r.status_code, r.text))
        except Exception as e:
            _log("ping: %s" % e)
        return False

    start_daemon(command=args, environment=env, is_alive_check=check, pid_file_name=TVMTOOL_PID_FILE)


def stop(argv):
    with open(TVMTOOL_PID_FILE) as f:
        pid = f.read()
    if not stop_daemon(pid):
        _log("pid is dead: %s" % pid)


def _log(msg):
    print("%s : tvmtool-recipe : %s" % (datetime.datetime.now(), msg), file=sys.stdout)


if __name__ == "__main__":
    try:
        declare_recipe(start, stop)
    except Exception as e:
        _log("exception: %s" % e)
