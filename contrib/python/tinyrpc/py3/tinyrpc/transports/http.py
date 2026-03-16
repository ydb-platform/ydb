#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Callable, Dict

import requests

from . import ClientTransport


class HttpPostClientTransport(ClientTransport):
    """HTTP POST based client transport.

    Requires :py:mod:`requests`. Submits messages to a server using the body of
    an ``HTTP`` ``POST`` request. Replies are taken from the responses body.

    :param str endpoint: The URL to send ``POST`` data to.
    :param callable post_method: allows to replace `requests.post` with another method,
        e.g. the post method of a `requests.Session()` instance.
    :param dict kwargs: Additional parameters for :py:func:`requests.post`.
    """
    def __init__(
            self, endpoint: str, post_method: Callable = None, **kwargs: Dict
    ):
        self.endpoint = endpoint
        self.request_kwargs = kwargs
        if post_method is None:
            self.post = requests.post
        else:
            self.post = post_method

    def send_message(self, message: bytes, expect_reply: bool = True):
        if not isinstance(message, bytes):
            raise TypeError('message must by of type bytes')

        r = self.post(self.endpoint, data=message, **self.request_kwargs)

        if expect_reply:
            # Note that this is not strictly conforming to the (JSON RPC) standard since
            # even notifications may, under certain conditions, return an
            # error message which is completely ignored by this implementation.
            return r.content
