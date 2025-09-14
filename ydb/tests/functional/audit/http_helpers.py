# -*- coding: utf-8 -*-
import re
import requests

from helpers import cluster_http_endpoint


def extract_tablet_id(html_content, tablet_type='SCHEMESHARD'):
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8')
    # Find tablet_type
    # String like '<a href="tablets?TabletID=72057594046678944">SCHEMESHARD</a>'
    match = re.search(f'<a href="tablets\\?TabletID=(\\d+)">{tablet_type}</a>', html_content)
    if match:
        return match.group(1)
    return None


def authorization_headers(token):
    return {'Authorization': token}


def get_tablets_request(cluster, token):
    return requests.get(f'http://{cluster_http_endpoint(cluster)}/tablets', headers=authorization_headers(token))


def kill_tablet_request(cluster, tablet_id, token):
    return requests.get(f'http://{cluster_http_endpoint(cluster)}/tablets?RestartTabletID={tablet_id}', headers=authorization_headers(token))
