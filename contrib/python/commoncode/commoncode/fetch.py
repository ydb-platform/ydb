#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import os

import requests
from requests.exceptions import ConnectionError
from requests.exceptions import InvalidSchema

from commoncode import fileutils

logger = logging.getLogger(__name__)
# import sys
# logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
# logger.setLevel(logging.DEBUG)


def download_url(url, file_name=None, verify=True, timeout=10):
    """
    Fetch `url` and return the temporary location where the fetched content was
    saved. Use `file_name` if provided or create a new `file_name` base on the last
    url segment. If `verify` is True, SSL certification is performed. Otherwise, no
    verification is done but a warning will be printed.
    `timeout` is the timeout in seconds.
    """
    requests_args = dict(timeout=timeout, verify=verify)
    file_name = file_name or fileutils.file_name(url)

    try:
        response = requests.get(url, **requests_args)
    except (ConnectionError, InvalidSchema) as e:
        logger.error('download_url: Download failed for %(url)r' % locals())
        raise

    status = response.status_code
    if status != 200:
        msg = 'download_url: Download failed for %(url)r with %(status)r' % locals()
        logger.error(msg)
        raise Exception(msg)

    tmp_dir = fileutils.get_temp_dir(prefix='fetch-')
    output_file = os.path.join(tmp_dir, file_name)
    with open(output_file, 'wb') as out:
        out.write(response.content)

    return output_file


def ping_url(url):
    """
    Returns True is `url` is reachable.
    """
    try:
        from urlib.request import urlopen
    except ImportError:
        from urllib2 import urlopen

    # FIXME: if there is no 200 HTTP status, then the ULR may not be reachable.
    try:
        urlopen(url)
    except Exception:
        return False
    else:
        return True
