from collections import Counter
import pytest
import requests
import time

import asyncio
import httpx

import yatest.common
import yatest.common.network


TIMEOUT = 5000


@pytest.fixture(scope="session")
def running_example():
    with yatest.common.network.PortManager() as pm:
        port = pm.get_port()

        cmd = [
            yatest.common.binary_path("yt/yt/library/ytprof/example/ytprof-example"),
            str(port)
        ]

        p = yatest.common.execute(cmd, wait=False, env={"YT_LOG_LEVEL": "DEBUG"})
        time.sleep(1)
        assert p.running

        try:
            yield {"port": port}
        finally:
            p.kill()


def fetch_data(running_example, name):
    rsp = requests.get(f"http://localhost:{running_example['port']}/{name}")
    if rsp.status_code == 200:
        return rsp.content

    if rsp.status_code == 500:
        raise Exception(rsp.text)

    rsp.raise_for_status()


def test_smoke_tcmalloc(running_example):
    fetch_data(running_example, "heap")
    fetch_data(running_example, "allocations?d=1")
    fetch_data(running_example, "peak")
    fetch_data(running_example, "fragmentation")


async def get_async(url):
    async with httpx.AsyncClient() as client:
        return await client.get(url, timeout=TIMEOUT)


async def launch(running_example, name):
    url = f"http://localhost:{running_example['port']}/{name}"

    urls = [url, url, url]

    resps = await asyncio.gather(*map(get_async, urls))
    data = [resp.status_code for resp in resps]

    assert Counter(data) == Counter([200, 429, 429])


def test_async(running_example):
    fetch_data(running_example, "heap")
    asyncio.run(launch(running_example, "heap"))
    fetch_data(running_example, "heap")


def test_status_handlers(running_example):
    assert fetch_data(running_example, "buildid")
    assert fetch_data(running_example, "version")


def test_cpu_profile(running_example):
    if yatest.common.context.build_type != "profile":
        pytest.skip()

    fetch_data(running_example, "profile?d=1")
    fetch_data(running_example, "profile?d=1&freq=1000")


def test_spinlock_profile(running_example):
    if yatest.common.context.build_type != "profile":
        pytest.skip()

    fetch_data(running_example, "lock?d=1")
    fetch_data(running_example, "lock?d=1&frac=1")


def test_block_profile(running_example):
    if yatest.common.context.build_type != "profile":
        pytest.skip()

    fetch_data(running_example, "block?d=1")
    fetch_data(running_example, "block?d=1&frac=1")


def test_binary_handler(running_example):
    binary = fetch_data(running_example, "binary")

    with open(yatest.common.binary_path("yt/yt/library/ytprof/example/ytprof-example"), "rb") as f:
        real_binary = f.read()

    assert binary == real_binary

    with pytest.raises(Exception):
        fetch_data(running_example, "binary?check_build_id=1234")
