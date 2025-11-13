#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import math
import os
import re
import requests
from retry import retry_call
import urllib

timeout = 15
max_tries = 5
retry_delay = 2


def __parse_selectors(selectors):
    LABEL_NAME_PATTERN        = " *[a-zA-Z0-9-._/]{1,50} *"
    LABEL_VALUE_PATTERN       = " *[\"'][ -!#-&(-)+->@-_a-{}-~*|-]{1,200}[\"'] *"
    OPERATOR_PATTERN          = "=|!=|==|!==|=~|!~"

    SELECTOR_PATTERN          = "(?:(" + LABEL_NAME_PATTERN + ")(" + OPERATOR_PATTERN + ")(" + LABEL_VALUE_PATTERN + "))";
    SELECTORS_FULL_PATTERN    = "{((" + SELECTOR_PATTERN + ",)*" + SELECTOR_PATTERN + ")?}";

    result = {}
    if not re.fullmatch(SELECTORS_FULL_PATTERN, selectors):
        return result

    selector_values = re.findall(SELECTOR_PATTERN, selectors)
    for (name, op, value) in selector_values:
        result[name.strip()] = (op, value.strip(" '\""))

    return result


def _do_request_inner(method, url, json, headers):
    resp = requests.request(method=method, url=url, timeout=timeout, json=json, headers=headers)
    resp.raise_for_status()
    return resp


def _do_request(method, url, json=None, headers=None):
    return retry_call(_do_request_inner, fkwargs={"method": method, "url": url, "json": json, "headers": headers}, tries=max_tries, delay=retry_delay)


def _build_headers(args):
    token = os.environ.get("TOKEN", None)

    headers = {}
    headers["accept"] = "application/json;charset=UTF-8"
    if token is not None:
        if args.source_type == "SOLOMON":
            headers["Authorization"] = "OAuth {}".format(token)
        elif args.source_type == "MONITORING":
            headers["Authorization"] = "Bearer {}".format(token)

    return headers


def __do_sensors_request(args, page, pageSize):
    url = "https://{}/api/v2/projects/{}/sensors?".format(args.http_location, args.project)

    request_params = {
        "projectId": args.project,
        "selectors": args.selectors,
        "forceCluster": args.default_replica,
        "from": args.from_range,
        "to": args.to_range,
        "page": page,
        "pageSize": pageSize
    }

    return _do_request(method="GET", url=url + urllib.parse.urlencode(request_params), json=None, headers=_build_headers(args))


def __do_sensors_names_request(args):
    url = "https://{}/api/v2/projects/{}/sensors/names?".format(args.http_location, args.project)

    request_params = {
        "projectId": args.project,
        "selectors": args.selectors,
        "from": args.from_range,
        "to": args.to_range
    }

    return _do_request(method="GET", url=url + urllib.parse.urlencode(request_params), json=None, headers=_build_headers(args))


def get_sensors(args):
    response = __do_sensors_request(args, 0, 1)
    pages_count = math.ceil(response.json()["page"]["totalCount"] / args.page_size)

    current_page = 0
    metrics = []

    while (current_page < pages_count):
        response = __do_sensors_request(args, current_page)
        response_data = response.json()

        for metric in response_data["result"]:
            metrics.append(metric)

        current_page += 1

    return metrics


def get_sensor_names(args):
    response = __do_sensors_names_request(args)
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
    sensors_parser.set_defaults(command=get_sensors)

    sensors_parser.add_argument("--page-size", type=int, required=False, default=10000, help="List metrics query page size")

    label_names_parser = subparsers.add_parser(
        'names',
        formatter_class=argparse.RawTextHelpFormatter,
        description="""list possible label names for specified selectors"""
    )
    label_names_parser.set_defaults(command=get_sensor_names)

    source_type_choices = ["SOLOMON", "MONITORING"]

    parser.add_argument("--http-location", type=str, required=False, default="solomon.yandex.net", help="Solomon installation http endpoint")
    parser.add_argument("--grpc-location", type=str, required=False, default="solomon.yandex.net", help="Solomon installation grpc endpoint")
    parser.add_argument("--default-replica", type=str, required=False, default="sas", help="Solomon default replica")
    parser.add_argument("--source-type", choices=source_type_choices, default="SOLOMON", help="Solomon source type")
    parser.add_argument("-S", "--selectors", type=str, required=True, help="Selectors query")
    parser.add_argument("-F", "--from-range", type=str, required=False, default=(datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"), help="Left time range border")
    parser.add_argument("-T", "--to-range", type=str, required=False, default=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), help="Right time range border")
    args = parser.parse_args()

    args.parsed_selectors = __parse_selectors(args.selectors)
    if "project" not in args.parsed_selectors:
        return "project label should be specified in selectors"
    args.project = args.parsed_selectors["project"][1]

    return args

def main():
    args = parse_args()
    result = args.command(args)

    print(result)
