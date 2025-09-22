# -*- coding: utf-8 -*-
import re
import requests
import json

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


def sql_request(cluster, database, sql, token):
    headers = authorization_headers(token)
    headers['content-type'] = 'application/json'
    request = {
        'action': 'execute-query',
        'base64': False,
        'database': database,
        'query': sql,
        'stats': 'full',
        'syntax': 'yql_v1',
        'tracingLevel': 9,
    }
    return requests.post(f'http://{cluster_http_endpoint(cluster)}/viewer/json/query?schema=multi&base64=false', data=json.dumps(request), headers=headers)


def list_pdisks_request(cluster, token):
    return requests.get(f'http://{cluster_http_endpoint(cluster)}/actors/pdisks/', headers=authorization_headers(token))


def extract_pdisk(html_content):
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8')
    # Find PDisk
    # <a href='pdisk000000001'>PDisk000000001</a>
    match = re.search('<a href=\'(pdisk\\d+)\'>PDisk\\d+</a>', html_content)
    if match:
        return match.group(1)
    return None


def restart_pdisk(cluster, pdisk_subpage, token):
    return requests.post(f'http://{cluster_http_endpoint(cluster)}/actors/pdisks/{pdisk_subpage}', data='restartPDisk=&ignoreChecks=true', headers=authorization_headers(token))
