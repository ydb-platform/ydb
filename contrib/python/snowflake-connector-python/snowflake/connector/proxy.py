#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import os


def set_proxies(proxy_host, proxy_port, proxy_user=None, proxy_password=None):
    """Sets proxy dict for requests."""
    PREFIX_HTTP = "http://"
    PREFIX_HTTPS = "https://"
    proxies = None
    if proxy_host and proxy_port:
        if proxy_host.startswith(PREFIX_HTTP):
            proxy_host = proxy_host[len(PREFIX_HTTP) :]
        elif proxy_host.startswith(PREFIX_HTTPS):
            proxy_host = proxy_host[len(PREFIX_HTTPS) :]
        if proxy_user or proxy_password:
            proxy_auth = "{proxy_user}:{proxy_password}@".format(
                proxy_user=proxy_user if proxy_user is not None else "",
                proxy_password=proxy_password if proxy_password is not None else "",
            )
        else:
            proxy_auth = ""
        proxies = {
            "http": "http://{proxy_auth}{proxy_host}:{proxy_port}".format(
                proxy_host=proxy_host,
                proxy_port=str(proxy_port),
                proxy_auth=proxy_auth,
            ),
            "https": "http://{proxy_auth}{proxy_host}:{proxy_port}".format(
                proxy_host=proxy_host,
                proxy_port=str(proxy_port),
                proxy_auth=proxy_auth,
            ),
        }
        os.environ["HTTP_PROXY"] = proxies["http"]
        os.environ["HTTPS_PROXY"] = proxies["https"]
    return proxies
