# -*- coding: utf-8 -*-
import argparse
import json
import re
import shlex
from collections import OrderedDict, namedtuple

from six.moves import http_cookies as Cookie

parser = argparse.ArgumentParser()
parser.add_argument('command')
parser.add_argument('url')
parser.add_argument('-d', '--data')
parser.add_argument('-b', '--data-binary', '--data-raw', default=None)
parser.add_argument('-X', default='')
parser.add_argument('-H', '--header', action='append', default=[])
parser.add_argument('--compressed', action='store_true')
parser.add_argument('-k','--insecure', action='store_true')
parser.add_argument('--user', '-u', default=())
parser.add_argument('-i','--include', action='store_true')
parser.add_argument('-s','--silent', action='store_true')

BASE_INDENT = " " * 4

ParsedContext = namedtuple('ParsedContext', ['method', 'url', 'data', 'headers', 'cookies', 'verify', 'auth'])

def parse_context(curl_command):
    method = "get"

    tokens = shlex.split(curl_command)
    parsed_args = parser.parse_args(tokens)

    post_data = parsed_args.data or parsed_args.data_binary
    if post_data:
        method = 'post'

    if parsed_args.X:
        method = parsed_args.X.lower()

    cookie_dict = OrderedDict()
    quoted_headers = OrderedDict()

    for curl_header in parsed_args.header:
        if curl_header.startswith(':'):
            occurrence = [m.start() for m in re.finditer(':', curl_header)]
            header_key, header_value = curl_header[:occurrence[1]], curl_header[occurrence[1] + 1:]
        else:
            header_key, header_value = curl_header.split(":", 1)

        if header_key.lower().strip("$") == 'cookie':
            cookie = Cookie.SimpleCookie(header_value)
            for key in cookie:
                cookie_dict[key] = cookie[key].value
        else:
            quoted_headers[header_key] = header_value.strip()

    # add auth
    user = parsed_args.user
    if parsed_args.user:
        user = tuple(user.split(':'))

    return ParsedContext(
        method=method,
        url=parsed_args.url,
        data=post_data,
        headers=quoted_headers,
        cookies=cookie_dict,
        verify=parsed_args.insecure,
        auth=user
    )


def parse(curl_command, **kargs):
    parsed_context = parse_context(curl_command)

    data_token = ''
    if parsed_context.data:
        data_token = '{}data=\'{}\',\n'.format(BASE_INDENT, parsed_context.data)

    verify_token = ''
    if parsed_context.verify:
        verify_token = '\n{}verify=False'.format(BASE_INDENT)

    requests_kargs=''
    for k,v in sorted(kargs.items()):
        requests_kargs += "{}{}={},\n".format(BASE_INDENT,k,str(v))

    #auth_data = f'{BASE_INDENT}auth={parsed_context.auth}'
    auth_data = "{}auth={}".format(BASE_INDENT,parsed_context.auth)

    formatter = {
        'method': parsed_context.method,
        'url': parsed_context.url,
        'data_token': data_token,
        'headers_token': "{}headers={}".format(BASE_INDENT, dict_to_pretty_string(parsed_context.headers)),
        'cookies_token': "{}cookies={}".format(BASE_INDENT, dict_to_pretty_string(parsed_context.cookies)),
        'security_token': verify_token,
        'requests_kargs': requests_kargs,
        'auth': auth_data
    }

    return """requests.{method}("{url}",
{requests_kargs}{data_token}{headers_token},
{cookies_token},
{auth},{security_token}
)""".format(**formatter)


def dict_to_pretty_string(the_dict, indent=4):
    if not the_dict:
        return "{}"

    return ("\n" + " " * indent).join(
        json.dumps(the_dict, sort_keys=True, indent=indent, separators=(',', ': ')).splitlines())

