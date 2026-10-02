###
# from here https://github.com/ydb-platform/benchhelpers/blob/main/tpcc/ydb/table_full_compact.py
###

import allure
import os
import re
import time
import requests
import logging

from urllib.parse import quote_plus
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from multiprocessing.pool import ThreadPool


URL_TABLE_DESCRIPTION = '{url_base}/viewer/json/describe?path={path}&enums=true'
URL_EXECUTOR_INTERNALS = '{url_base}/tablets/executorInternals?TabletID={tablet_id}'
URL_FORCE_COMPACT = '{url_base}/tablets/executorInternals?TabletID={tablet_id}&force_compaction={local_table_id}'
RE_DBASE_SIZE = re.compile(r'DBase{.*?, (\d+)\)b}', re.S)
RE_LOANED_PARTS = re.compile(r'<h4>Loaned parts</h4><pre>(.*?)</pre>', re.S)
RE_FORCED_COMPACTION_STATE = re.compile(r'Forced compaction: (\w+)', re.S)


def get_headers() -> dict[str, str]:
    result = {}
    oauth = os.getenv('OLAP_YDB_OAUTH')
    if oauth:
        result['Authorization'] = 'OAuth ' + oauth
    elif YdbCluster.ydb_iam_file:
        with open(YdbCluster.ydb_iam_file) as token_file:
            token = token_file.read().strip()
        result['Authorization'] = 'Bearer ' + token
    return result


def load_json(url):
    response = requests.get(url, headers=get_headers(), verify=False, timeout=30)
    response.raise_for_status()
    return response.json()


def describe_table(path):
    url = URL_TABLE_DESCRIPTION.format(url_base=YdbCluster._get_service_url(), path=quote_plus(YdbCluster.get_full_tables_path(path)))
    return load_json(url)


def tablet_internals(tablet_id):
    url = URL_EXECUTOR_INTERNALS.format(url_base=YdbCluster._get_service_url(), tablet_id=tablet_id)
    response = requests.get(url, headers=get_headers(), verify=False, timeout=30)
    response.raise_for_status()
    return response.text


def extract_loaned_parts(text):
    m = RE_LOANED_PARTS.search(text)
    if m:
        return m.group(1).split()
    else:
        return None


def extract_force_compaction_state(text):
    m = RE_FORCED_COMPACTION_STATE.search(text)
    if m:
        return m.group(1)
    else:
        return None


def start_force_compaction(tablet_id, local_table_id=1001):
    url = URL_FORCE_COMPACT.format(url_base=YdbCluster._get_service_url(), tablet_id=tablet_id, local_table_id=local_table_id)
    response = requests.get(url, headers=get_headers(), verify=False, timeout=30)
    response.raise_for_status()
    text = response.text
    if 'Table will be compacted in the near future' not in text:
        logging.warning(text)


def force_compact(tablet_id, timeout: float, local_table_id=1001):
    state = extract_force_compaction_state(tablet_internals(tablet_id))
    if state is None:
        start_force_compaction(tablet_id, local_table_id)
        time.sleep(0.1)
    start_time = time.time()
    while time.time() - start_time < timeout:
        prev_state = state
        state = extract_force_compaction_state(tablet_internals(tablet_id))
        if state is None:
            return
        if state != 'Compacting' and state != prev_state:
            logging.info(f'... {state}')
        time.sleep(1)
    raise TimeoutError(f'Compacting tablet {tablet_id} timeout ({timeout}s).')


@allure.step
def force_datashard_compact_legacy(tables: list[str], timeout: float, threads: int = 100):
    tablet_ids = []
    for table in tables:
        for p in describe_table(table)['PathDescription']['TablePartitions']:
            tablet_ids.append(int(p['DatashardId']))
    tablet_ids.sort()

    def generate_tasks():
        for i, tablet_id in enumerate(tablet_ids):
            yield i + 1, len(tablet_ids), tablet_id

    def process_task(task):
        index, count, tablet_id = task
        tablet_url = URL_EXECUTOR_INTERNALS.format(url_base=YdbCluster._get_service_url(), tablet_id=tablet_id)
        logging.info(f'[{time.ctime()}] [{index}/{count}] Compacting {tablet_id} url: {tablet_url}')
        force_compact(tablet_id, timeout=timeout)
        if extract_loaned_parts(tablet_internals(tablet_id)):
            logging.info(f'[{time.ctime()}] [{index}/{count}] !!! WARNING !!! Tablet {tablet_id} has loaned parts after compaction')

    with ThreadPool(threads) as pool:
        for _ in pool.imap_unordered(process_task, generate_tasks()):
            pass
