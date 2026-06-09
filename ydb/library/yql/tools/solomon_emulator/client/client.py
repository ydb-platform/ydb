#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
from retry import retry_call

timeout = 15
max_tries = 5
retry_delay = 2


def get_api_url():
    return "http://localhost:{}".format(os.environ['SOLOMON_HTTP_PORT'])


def _do_request_inner(method, url, json):
    resp = requests.request(method=method, url=url, timeout=timeout, json=json)
    resp.raise_for_status()
    return resp


def _do_request(method, url, json=None):
    return retry_call(_do_request_inner, fkwargs={"method": method, "url": url, "json": json}, tries=max_tries, delay=2)


def config_solomon(response_code):
    url = "{url}/config".format(
        url=get_api_url())
    _do_request("POST", url, {"response_code": response_code})


def cleanup_emulator():
    _do_request("POST", "{url}/cleanup".format(url=get_api_url()))


def cleanup_solomon(project, cluster, service):
    url = "{url}/cleanup?project={project}&cluster={cluster}&service={service}".format(
        url=get_api_url(),
        project=project,
        cluster=cluster,
        service=service)
    _do_request("POST", url)


def cleanup_monitoring(folderId, service):
    cleanup_solomon(folderId, folderId, service)


def add_solomon_metrics(project, cluster, service, metrics):
    url = "{url}/metrics/post?project={project}&cluster={cluster}&service={service}".format(
        url=get_api_url(),
        project=project,
        cluster=cluster,
        service=service)
    _do_request("POST", url, metrics)


def add_monitoring_metrics(folderId, service, metrics):
    return add_solomon_metrics(folderId, folderId, service, metrics)


def get_solomon_metrics(project, cluster, service):
    url = "{url}/metrics/get?project={project}&cluster={cluster}&service={service}".format(
        url=get_api_url(),
        project=project,
        cluster=cluster,
        service=service)
    return sorted(_do_request("GET", url).json(), key=lambda x : x['ts'])


def get_monitoring_metrics(folderId, service):
    return get_solomon_metrics(folderId, folderId, service)
