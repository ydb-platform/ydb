# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
import requests
from urllib.parse import urlencode
import time


cluster = KiKiMR(KikimrConfigGenerator())
cluster.start()


def call_viewer_api(url):
    port = cluster.nodes[1].mon_port
    return requests.get("http://localhost:%s%s" % (port, url))


def call_viewer(url, params=None):
    if params is None:
        params = {}
    return call_viewer_api(url + '?' + urlencode(params))


def call_viewer_db(url, params=None):
    if params is None:
        params = {}
    params["database"] = cluster.domain_name
    return call_viewer(url, params)


def get_result(result):
    if result.status_code == 200 and result.headers.get("Content-Type") == "application/json":
        return result.json()
    return {"status_code": result.status_code, "text": result.text}


def get_viewer(url, params=None):
    if params is None:
        params = {}
    return get_result(call_viewer(url, params))


def get_viewer_db(url, params=None):
    if params is None:
        params = {}
    return get_result(call_viewer_db(url, params))


wait_good = False
wait_time = 0


def wait_for_cluster_ready():
    global wait_time
    global wait_good
    for node_id, node in cluster.nodes.items():
        while wait_time < 60:
            all_good = False
            while True:
                try:
                    print("Waiting for node %s to be ready" % node_id)
                    result_counter = get_result(requests.get("http://localhost:%s/viewer/simple_counter?max_counter=1&period=1" % node.mon_port))  # check that handlers are ready
                    if result_counter['status_code'] != 200:
                        break
                    result = get_result(requests.get("http://localhost:%s/viewer/sysinfo?node_id=." % node.mon_port))  # check that stats are ready
                    if 'status_code' in result and result.status_code != 200:
                        break
                    if 'SystemStateInfo' not in result or len(result['SystemStateInfo']) == 0:
                        break
                    sys_info = result['SystemStateInfo'][0]
                    if 'Roles' not in sys_info:
                        break
                    if 'MemoryUsed' not in sys_info:
                        break
                    if 'Storage' in sys_info['Roles'] and 'MaxDiskUsage' not in sys_info:
                        break
                    print("Node %s is ready" % node_id)
                    all_good = True
                except requests.exceptions.ConnectionError:
                    pass
                break

            if all_good:
                break
            time.sleep(1)
            wait_time += 1
    while wait_time < 60:
        all_good = False
        while True:
            result = get_result(requests.get("http://localhost:%s/storage/groups" % cluster.nodes[1].mon_port))  # check that stats are ready
            if 'status_code' in result and result['status_code'] != 200:
                break
            if 'TotalGroups' not in result or result['TotalGroups'] < 3:
                break
            all_good = True
            break
        if all_good:
            break
        time.sleep(1)
        wait_time += 1

    wait_good = wait_time < 60
    print('Wait for cluster to be ready took %s seconds' % wait_time)


wait_for_cluster_ready()


def test_wait_for_cluster_ready():
    return {"wait_good": wait_good}


def test_counter():
    return get_viewer("/viewer/simple_counter", {'max_counter': 1})


def replace_values_by_key(data, target_key):
    def can_convert_to_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def replacement_value(value):
        if isinstance(value, int):
            return "number"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
            if can_convert_to_number(value):
                return "number-text"
            return "text"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, list):
            return "array"
        return value

    def replace_recursive(data):
        if isinstance(data, dict):
            return {key: replace_recursive(replacement_value(value) if key in target_key else value)
                    for key, value in data.items()}
        elif isinstance(data, list):
            return [replace_recursive(item) for item in data]
        else:
            return data

    return replace_recursive(data)


def normalize_result_pdisks(result):
    return replace_values_by_key(result, ['AvailableSize',
                                          'TotalSize',
                                          'LogUsedSize',
                                          'LogTotalSize',
                                          'SystemSize',
                                          ])


def normalize_result_vdisks(result):
    return replace_values_by_key(result, ['AvailableSize',
                                          'IncarnationGuid',
                                          'InstanceGuid',
                                          'WriteThroughput',
                                          'ReadThroughput',
                                          ])


def normalize_result_groups(result):
    return replace_values_by_key(result, ['Read',
                                          'Write',
                                          ])


def normalize_result_nodes(result):
    return replace_values_by_key(result, ['CpuUsage',
                                          'DiskSpaceUsage',
                                          'Address',
                                          'Port',
                                          'port',
                                          'host',
                                          'Host',
                                          'LoadAverage',
                                          'Usage',
                                          'MemoryStats',
                                          'MemoryUsed',
                                          'MemoryTotal',
                                          'MemoryLimit',
                                          'NumberOfCpus',
                                          'Version',
                                          'UptimeSeconds',
                                          'CoresUsed',
                                          'CoresTotal',
                                          'CreateTime',
                                          'MaxDiskUsage',
                                          'Roles',
                                          ])


def normalize_result_info(result):
    return replace_values_by_key(result, ['ChangeTime',
                                          'StartTime',
                                          'ResponseTime',
                                          'ResponseDuration',
                                          ])


def normalize_result_schema(result):
    return replace_values_by_key(result, ['CreateStep',
                                          'ACL',
                                          'EffectiveACL'
                                          ])


def normalize_result_cluster(result):
    return replace_values_by_key(result, ['MapVersions',
                                          'Versions',
                                          ])


def normalize_result(result):
    result = normalize_result_nodes(result)
    result = normalize_result_info(result)
    result = normalize_result_schema(result)
    result = normalize_result_groups(result)
    result = normalize_result_pdisks(result)
    result = normalize_result_vdisks(result)
    result = normalize_result_cluster(result)
    return result


def get_viewer_normalized(url, params=None):
    return normalize_result(get_viewer(url, params))


def get_viewer_db_normalized(url, params=None):
    return normalize_result(get_viewer_db(url, params))


def test_viewer_nodes():
    return get_viewer_normalized("/viewer/nodes", {
        'fields_required': 'all'
    })


def test_storage_groups():
    return get_viewer_normalized("/storage/groups", {
        'fields_required': 'all'
    })


def test_viewer_nodeinfo():
    return get_viewer_normalized("/viewer/nodeinfo")


def test_viewer_sysinfo():
    return get_viewer_normalized("/viewer/sysinfo")


def test_viewer_vdiskinfo():
    return get_viewer_normalized("/viewer/vdiskinfo")


def test_viewer_pdiskinfo():
    return get_viewer_normalized("/viewer/pdiskinfo")


def test_viewer_bsgroupinfo():
    return get_viewer_normalized("/viewer/bsgroupinfo")


def test_viewer_tabletinfo():
    return get_viewer_db_normalized("/viewer/tabletinfo")


def test_viewer_describe():
    db = cluster.domain_name
    return get_viewer_db_normalized("/viewer/describe", {'path': db})


def test_viewer_cluster():
    return get_viewer_normalized("/viewer/cluster")


def test_viewer_tenantinfo():
    return get_viewer_normalized("/viewer/tenantinfo")


def test_viewer_tenantinfo_db():
    return get_viewer_db_normalized("/viewer/tenantinfo")


def test_viewer_healthcheck():
    return get_viewer_normalized("/viewer/healthcheck")


def test_viewer_acl():
    db = cluster.domain_name
    return get_viewer_db("/viewer/acl", {'path': db})


def test_viewer_autocomplete():
    return get_viewer_db("/viewer/autocomplete", {'prefix': ''})


def test_viewer_check_access():
    db = cluster.domain_name
    return get_viewer_db("/viewer/check_access", {'path': db, 'permissions': 'read'})


def test_viewer_query():
    return get_viewer_db("/viewer/query", {'query': 'select 7*6', 'schema': 'multi'})


def test_viewer_query_issue_13757():
    return get_viewer_db("/viewer/query", {
        'query': 'SELECT CAST(<|one:"8912", two:42|> AS Struct<two:Utf8, three:Date?>);',
        'schema': 'multi'
    })
