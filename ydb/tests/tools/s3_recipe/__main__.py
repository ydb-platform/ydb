#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests

from library.python.testing.recipe import declare_recipe, set_env
from library.recipes import common as recipes_common
from yatest.common.network import PortManager
import yatest.common
import os

PID_FILENAME = "s3_recipe.pid"
MOTO_SERVER_PATH = os.getenv("MOTO_SERVER_EXECUTABLE") or "contrib/python/moto/bin/moto_server"


def start(argv):
    logging.debug("Starting S3 recipe")
    pm = PortManager()
    port = pm.get_port()
    url = "http://localhost:{port}".format(port=port)  # S3 libs require DNS name for S3 endpoint
    check_url = "http://[::1]:{port}".format(port=port)
    cmd = [
        yatest.common.binary_path(MOTO_SERVER_PATH),
        "s3",
        "--host", "::1",
        "--port", str(port)
    ]

    def is_s3_ready():
        try:
            response = requests.get(check_url)
            response.raise_for_status()
            return True
        except requests.RequestException as err:
            logging.debug(err)
            return False

    recipes_common.start_daemon(
        command=cmd,
        environment=None,
        is_alive_check=is_s3_ready,
        pid_file_name=PID_FILENAME
    )

    set_env("S3_ENDPOINT", url)
    logging.debug(f"S3 recipe has been started, url: {url}")


def stop(argv):
    logging.debug("Stop S3 recipe")
    with open(PID_FILENAME, "r") as f:
        pid = int(f.read())
        recipes_common.stop_daemon(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
