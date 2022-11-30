import argparse
import datetime
import json
import os
import requests
import sys

import yatest.common
from library.python.testing.recipe import declare_recipe
from library.recipes.common import start_daemon, stop_daemon
from yatest.common import network

TIROLE_PORT_FILE = "tirole.port"
TIROLE_PID_FILE = "tirole.pid"

CONFIG_PATH = './tirole.config.json'


PORT_MANAGER = network.PortManager()


def _gen_config(roles_dir):
    http_port = PORT_MANAGER.get_tcp_port(80)

    cfg = {
        "http_common": {
            "listen_address": "localhost",
            "port": http_port,
        },
        "logger": {
            "file": yatest.common.output_path("tirole-common.log"),
        },
        "service": {
            "common": {
                "access_log": yatest.common.output_path("tirole-access.log"),
            },
            "tvm": {
                "self_tvm_id": 1000001,
            },
            "key_map": {
                "keys_file": yatest.common.source_path("library/recipes/tirole/data/sign.keys"),
                "default_key": "1",
            },
            "unittest": {
                "roles_dir": yatest.common.source_path(roles_dir) + "/",
            },
        },
    }

    with open(CONFIG_PATH, 'wt') as f:
        json.dump(cfg, f, sort_keys=True, indent=4)

    return http_port


def start(argv):
    _log('Starting Tirole recipe')

    parser = argparse.ArgumentParser()
    parser.add_argument('--roles-dir', dest='roles_dir', type=str, required=True)
    input_args = parser.parse_args(argv)

    http_port = _gen_config(input_args.roles_dir)

    print(http_port, file=sys.stderr)
    with open(TIROLE_PORT_FILE, "w") as f:
        f.write(str(http_port))

    # launch
    args = [
        yatest.common.build_path() + '/passport/infra/daemons/tirole/cmd/tirole',
        '-c',
        CONFIG_PATH,
    ]

    def check():
        try:
            r = requests.get("http://localhost:%d/ping" % http_port)
            if r.status_code == 200:
                return True
            else:
                _log("ping: %d : %s" % (r.status_code, r.text))
        except Exception as e:
            _log("ping: %s" % e)
        return False

    start_daemon(command=args, environment=os.environ.copy(), is_alive_check=check, pid_file_name=TIROLE_PID_FILE)


def stop(argv):
    with open(TIROLE_PID_FILE) as f:
        pid = f.read()
    if not stop_daemon(pid):
        _log("pid is dead: %s" % pid)


def _log(msg):
    print("%s : tirole-recipe : %s" % (datetime.datetime.now(), msg), file=sys.stdout)


if __name__ == "__main__":
    declare_recipe(start, stop)
