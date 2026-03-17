#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import os


def test_set_proxies():
    from snowflake.connector.proxy import set_proxies

    assert set_proxies("proxyhost", "8080") == {
        "http": "http://proxyhost:8080",
        "https": "http://proxyhost:8080",
    }
    assert set_proxies("http://proxyhost", "8080") == {
        "http": "http://proxyhost:8080",
        "https": "http://proxyhost:8080",
    }
    assert set_proxies("http://proxyhost", "8080", "testuser", "testpass") == {
        "http": "http://testuser:testpass@proxyhost:8080",
        "https": "http://testuser:testpass@proxyhost:8080",
    }
    assert set_proxies("proxyhost", "8080", "testuser", "testpass") == {
        "http": "http://testuser:testpass@proxyhost:8080",
        "https": "http://testuser:testpass@proxyhost:8080",
    }

    # NOTE environment variable is set if the proxy parameter is specified.
    del os.environ["HTTP_PROXY"]
    del os.environ["HTTPS_PROXY"]
