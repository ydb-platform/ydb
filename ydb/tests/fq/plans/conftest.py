#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pytest
import requests
import yatest.common
import library.python.port_manager
from dataclasses import dataclass

from library.recipes import common as recipes_common
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config

MOTO_SERVER_PATH = "contrib/python/moto/bin/moto_server"
S3_PID_FILE = "s3.pid"


@dataclass
class S3:
    s3_url: str


@pytest.fixture(scope="module")
def s3(request) -> S3:
    port_manager = library.python.port_manager.PortManager()
    s3_port = port_manager.get_port()
    s3_url = "http://localhost:{port}".format(port=s3_port)

    command = [yatest.common.binary_path(MOTO_SERVER_PATH), "s3", "--host", "::1", "--port", str(s3_port)]

    def is_s3_ready():
        try:
            response = requests.get(s3_url)
            response.raise_for_status()
            return True
        except requests.RequestException as err:
            logging.debug(err)
            return False

    recipes_common.start_daemon(
        command=command, environment=None, is_alive_check=is_s3_ready, pid_file_name=S3_PID_FILE
    )

    try:
        yield S3(s3_url)
    finally:
        with open(S3_PID_FILE, 'r') as f:
            pid = int(f.read())
        recipes_common.stop_daemon(pid)


@pytest.fixture
def kikimr(request, s3):
    cluster = Kikimr(get_ydb_config(request))
    try:
        cluster.wait_kqp_node_count(2)
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def client(kikimr):
    return kikimr.ydb_client
