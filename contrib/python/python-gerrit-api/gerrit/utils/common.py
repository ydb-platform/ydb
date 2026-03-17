#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import json


def strip_trailing_slash(url):
    """
    remove url's trailing slash
    :param url: url
    :return:
    """
    while url.endswith("/"):
        url = url[:-1]
    return url


def decode_response(response):
    """Strip off Gerrit's magic prefix and decode a response.
    :returns:
        Decoded JSON content as a dict, or raw text if content could not be
        decoded as JSON.
    :raises:
        requests.HTTPError if the response contains an HTTP error status code.
    """
    magic_json_prefix = ")]}'\n"
    content_type = response.headers.get("content-type", "")

    content = response.content.strip()
    if response.encoding:
        content = content.decode(response.encoding)
    if not content:
        return content
    if content_type.split(";")[0] != "application/json":
        return content
    if content.startswith(magic_json_prefix):
        index = len(magic_json_prefix)
        content = content[index:]
    try:
        return json.loads(content)
    except ValueError:
        raise ValueError(f"Invalid json content: {content}")


def params_creator(tuples, pattern_types, pattern_dispatcher):
    p, v = None, None
    if pattern_dispatcher is not None and pattern_dispatcher:
        for item in pattern_types:
            if item in pattern_dispatcher:
                p, v = pattern_types[item], pattern_dispatcher[item]
                break
        else:
            k = list(pattern_types.keys())
            raise ValueError(
                "Pattern types can be either " + ", ".join(k[:-1]) + " or " + k[-1]
            )

    params = {k: v for k, v in tuples + ((p, v),) if v is not None}

    return params
