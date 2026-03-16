import math
import re

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_simple(aresponses):
    aresponses.add("google.com", "/api/v1/", "GET", response="OK")
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="error", status=500))

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com/api/v1/") as response:
            text = await response.text()
            assert text == "OK"

        async with session.get("https://foo.com") as response:
            text = await response.text()
            assert text == "error"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_regex_repetition(aresponses):
    aresponses.add(re.compile(r".*\.?google\.com"), response="OK", repeat=2)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com") as response:
            text = await response.text()
            assert text == "OK"

        async with session.get("http://api.google.com") as response:
            text = await response.text()
            assert text == "OK"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_json(aresponses):
    aresponses.add("google.com", "/api/v1/", "GET", response={"status": "OK"})

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com/api/v1/") as response:
            assert {"status": "OK"} == await response.json()

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_handler(aresponses):
    def break_everything(request):
        return aresponses.Response(status=500, text=str(request.url))

    aresponses.add(response=break_everything, repeat=math.inf)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com/api/v1/") as response:
            assert response.status == 500
