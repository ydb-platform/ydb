#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import requests
from retry import retry_call
import urllib

timeout = 15
max_tries = 5
retry_delay = 2


def _do_request_inner(method, url, json, headers):
    resp = requests.request(method=method, url=url, timeout=timeout, json=json, headers=headers)
    resp.raise_for_status()
    return resp


def _do_request(method, url, json=None, headers=None):
    return retry_call(_do_request_inner, fkwargs={"method": method, "url": url, "json": json, "headers": headers}, tries=max_tries, delay=retry_delay)


def _build_headers():
    token = os.environ.get("TOKEN", None)

    headers = {}
    headers["accept"] = "application/json;charset=UTF-8"
    if token is not None:
        headers["Authorization"] = "OAuth {}".format(token)

    return headers


def list_sensors(args):
    url = "https://{}/api/v2/projects/{}/sensors?".format(args.http_location, args.project)

    request_params = {
        "projectId": args.project,
        "selectors": args.selectors,
        "forceCluster": args.default_replica,
        "from": args.from_range,
        "to": args.to_range,
        "page": 0,
        "pageSize": args.page_size
    }

    pages_count = 1
    current_page = 0

    metrics = []

    while (current_page < pages_count):
        request_params["page"] = current_page

        response = _do_request(method="GET", url=url + urllib.parse.urlencode(request_params), json=None, headers=_build_headers())
        response_data = response.json()

        for metric in response_data["result"]:
            metrics.append(metric)

        pages_count = response_data["page"]["pagesCount"]
        current_page += 1

    return metrics


def list_sensor_names(args):
    url = "https://{}/api/v2/projects/{}/sensors/names?".format(args.http_location, args.project)

    request_params = {
        "projectId": args.project,
        "selectors": args.selectors,
        "from": args.from_range,
        "to": args.to_range
    }

    response = _do_request(method="GET", url=url + urllib.parse.urlencode(request_params), json=None, headers=_build_headers())
    response_data = response.json()

    return response_data["names"]


def parse_args():
    program_name = 'solomon_client'
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="solomon client util"
    )
    subparsers = parser.add_subparsers(help='sub-command help', required=True)
    sensors_parser = subparsers.add_parser(
        'sensors',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""list metrics for specified selectors"""
    )
    sensors_parser.set_defaults(command=list_sensors)

    sensors_parser.add_argument("--page-size", type=int, required=False, default=10000, help="List metrics query page size")

    label_names_parser = subparsers.add_parser(
        'names',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""list possible label names for specified selectors"""
    )
    label_names_parser.set_defaults(command=list_sensor_names)

    parser.add_argument("--http-location", type=str, required=False, default="solomon.yandex.net", help="Solomon installation http endpoint")
    parser.add_argument("--grpc-location", type=str, required=False, default="solomon.yandex.net", help="Solomon installation grpc endpoint")
    parser.add_argument("--default-replica", type=str, required=False, default="sas", help="Solomon default replica")
    parser.add_argument("-P", "--project", type=str, required=True, help="Selectors project")
    parser.add_argument("-S", "--selectors", type=str, required=True, help="Selectors query")
    parser.add_argument("-F", "--from-range", type=str, required=False, default=(datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"), help="Left time range border")
    parser.add_argument("-T", "--to-range", type=str, required=False, default=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), help="Right time range border")
    return parser.parse_args()


def main():
    args = parse_args()
    result = args.command(args)

    print(result)
