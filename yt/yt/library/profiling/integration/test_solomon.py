import time
import pytest
import requests
import datetime

import yatest.common

from yatest.common import network


@pytest.fixture(scope="session")
def test_url():
    with network.PortManager() as pm:
        port = pm.get_port()

        cmd = [
            yatest.common.binary_path("yt/yt/library/profiling/example/profiling-example"),
            str(port),
            "--fast"
        ]

        p = yatest.common.execute(cmd, wait=False, env={"YT_LOG_LEVEL": "DEBUG"})
        time.sleep(20)
        assert p.running

        yield f"http://localhost:{port}"


def check_ok(url, headers={}):
    req = requests.get(url, headers=headers)
    req.raise_for_status()


def test_debug(test_url):
    check_ok(test_url + "/solomon/")
    check_ok(test_url + "/solomon/tags")
    check_ok(test_url + "/solomon/sensors")
    check_ok(test_url + "/solomon/status")

    status = requests.get(test_url + "/solomon/status").json()
    assert "yt/my_loop/invalid" in status["sensor_errors"]
    assert len(status["window"]) > 5


@pytest.mark.parametrize('format', ["application/json", "application/x-spack"])
def test_get_last(test_url, format):
    check_ok(test_url + "/solomon/all", headers={"Accept": format})
    check_ok(test_url + "/solomon/shard/internal", headers={"Accept": format})
    check_ok(test_url + "/solomon/shard/default", headers={"Accept": format})


def format_now(delta=None, grid=2):
    d = datetime.datetime.utcnow()
    if d.second % grid != 0:
        d -= datetime.timedelta(seconds=d.second % grid)

    if delta:
        d += delta

    # 2020-11-10T17:21:00.000000
    return d.strftime("%Y-%m-%dT%H:%M:%S")


@pytest.mark.parametrize('format', ["application/json", "application/x-spack"])
def test_timestamp_args(test_url, format):
    now = format_now()
    check_ok(test_url + f"/solomon/all?now={now}&period=10s", headers={"Accept": format})

    # test reply cache
    for i in range(10):
        check_ok(test_url + f"/solomon/all?now={now}&period=6s", headers={"Accept": format})


@pytest.mark.parametrize('format', ["application/json", "application/x-spack"])
def test_timestamp_grid(test_url, format):
    now = format_now()

    check_ok(test_url + f"/solomon/all?now={now}&period=10s", headers={"Accept": format, "X-Solomon-GridSec": "2"})
    check_ok(test_url + f"/solomon/all?now={now}&period=10s", headers={"Accept": format, "X-Solomon-GridSec": "10"})


def check_not_ok(url, headers={}):
    req = requests.get(url, headers=headers)
    assert req.status_code != 200


def test_invalid(test_url):
    now = format_now()
    unaligned_now = format_now(datetime.timedelta(seconds=1))
    future = format_now(datetime.timedelta(hours=6))
    past = format_now(datetime.timedelta(hours=-6))

    check_not_ok(test_url + f"/solomon/all?now={now}")
    check_not_ok(test_url + f"/solomon/all?now={unaligned_now}")
    check_not_ok(test_url + f"/solomon/all?now={now}&period=180s")
    check_not_ok(test_url + f"/solomon/all?now={past}&period=10s")
    check_not_ok(test_url + f"/solomon/all?now={future}&period=10s")

    check_not_ok(test_url + f"/solomon/all?now={now}&period=0s")
    check_not_ok(test_url + f"/solomon/all?now={now}&period=3s")
    check_not_ok(test_url + f"/solomon/all?now={now}&period=10s", headers={"X-Solomon-GridSec": "3"})
    check_not_ok(test_url + f"/solomon/all?now={now}&period=10s", headers={"X-Solomon-GridSec": "20"})
    check_not_ok(test_url + f"/solomon/all?now={now}&period=10s", headers={"X-Solomon-GridSec": "0"})
