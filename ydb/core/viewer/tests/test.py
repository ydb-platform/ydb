# -*- coding: utf-8 -*-

import ydb
from ydb._topic_writer.topic_writer import PublicMessage
from ydb.tests.library.harness.kikimr_runner import KiKiMR

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
import requests
from urllib.parse import urlencode
import time


cluster = KiKiMR(KikimrConfigGenerator(enable_alter_database_create_hive_first=True))
cluster.start()
domain_name = '/' + cluster.domain_name
dedicated_db = domain_name + "/dedicated_db"
shared_db = domain_name + "/shared_db"
serverless_db = domain_name + "/serverless_db"
print('Creating database %s' % dedicated_db)
cluster.create_database(dedicated_db,
                        storage_pool_units_count={
                            'hdd': 1
                        })
cluster.register_and_start_slots(dedicated_db, count=1)
cluster.wait_tenant_up(dedicated_db)
cluster.create_hostel_database(shared_db,
                               storage_pool_units_count={
                                   'hdd': 1
                               })
cluster.register_and_start_slots(shared_db, count=1)
cluster.wait_tenant_up(shared_db)
cluster.create_serverless_database(serverless_db, shared_db)
cluster.wait_tenant_up(serverless_db)
databases = [domain_name, dedicated_db, shared_db, serverless_db]


def call_viewer_api(url):
    port = cluster.nodes[1].mon_port
    return requests.get("http://localhost:%s%s" % (port, url))


def get_result(result):
    if result.status_code == 200 and result.headers.get("Content-Type") == "application/json":
        return result.json()
    return {"status_code": result.status_code, "text": result.text}


def call_viewer(url, params=None):
    if params is None:
        params = {}
    return get_result(call_viewer_api(url + '?' + urlencode(params)))


def call_viewer_db(url, params=None):
    if params is None:
        params = {}
    result = {}
    for name in databases:
        params["database"] = name
        result[name] = call_viewer(url, params)
    return result


def get_viewer(url, params=None):
    if params is None:
        params = {}
    return call_viewer(url, params)


def get_viewer_db(url, params=None):
    if params is None:
        params = {}
    return call_viewer_db(url, params)


wait_good = False
wait_time = 0
max_wait_time = 300


def wait_for_cluster_ready():
    global wait_time
    global wait_good
    for node_id, node in cluster.nodes.items():
        while wait_time < max_wait_time:
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
    for database in databases:
        if database != domain_name:
            call_viewer("/viewer/query", {
                'database': database,
                'query': 'create table table1(id int64, name text, primary key(id)))',
                'schema': 'multi'
            })
    for database in databases:
        while wait_time < max_wait_time:
            all_good = False
            print("Waiting for database %s to be ready" % database)
            while True:
                result = get_result(requests.get("http://localhost:%s/viewer/tenantinfo?database=%s" % (cluster.nodes[1].mon_port, database)))  # force connect between nodes
                if 'status_code' in result and result['status_code'] != 200:
                    break
                if 'CoresUsed' not in result['TenantInfo'][0]:
                    break
                result = get_result(requests.get("http://localhost:%s/viewer/healthcheck?database=%s" % (cluster.nodes[1].mon_port, database)))  # force connect between nodes
                if 'status_code' in result and result['status_code'] != 200:
                    break
                if result['self_check_result'] != 'GOOD':
                    break
                all_good = True
                break
            if all_good:
                break
            time.sleep(1)
            wait_time += 1
    while wait_time < max_wait_time:
        all_good = False
        while True:
            result = call_viewer("/viewer/query", {
                'database': domain_name,
                'query': 'SELECT * FROM `.sys/ds_vslots`',
            })
            if 'status_code' in result and result['status_code'] != 200:
                break
            bad = 0
            for vslot in result:
                if 'State' not in vslot or vslot['State'] != 'OK':
                    bad += 1
            if bad > 0:
                break
            result = get_result(requests.get("http://localhost:%s/storage/groups?fields_required=all" % (cluster.nodes[1].mon_port)))  # force connect between nodes
            if 'status_code' in result and result['status_code'] != 200:
                break
            if len(result['StorageGroups']) < 5:
                break
            result = get_result(requests.get("http://localhost:%s/viewer/cluster" % (cluster.nodes[1].mon_port)))  # force connect between nodes
            if 'status_code' in result and result['status_code'] != 200:
                break
            if 'StorageTotal' not in result or result['StorageTotal'] == 0:
                break
            if 'StorageUsed' not in result or result['StorageUsed'] == 0:
                break
            all_good = True
            break
        if all_good:
            break
        time.sleep(1)
        wait_time += 1
    wait_good = wait_time < max_wait_time
    print('Wait for cluster to be ready took %s seconds' % wait_time)


wait_for_cluster_ready()


def test_wait_for_cluster_ready():
    return {"wait_good": wait_good}


def test_counter():
    return get_viewer("/viewer/simple_counter", {'max_counter': 1})


def replace_values_by_key(data, target_key):
    def can_convert_to_number(s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def replacement_value(value):
        if isinstance(value, int):
            if value != 0:
                return "not-zero-number"
        if isinstance(value, float):
            if value != 0:
                return "not-zero-number"
        if isinstance(value, str):
            if can_convert_to_number(value):
                if int(value) != 0:
                    return "not-zero-number-text"
                else:
                    return "0"
            if len(value) > 0:
                return "text"
        if isinstance(value, dict):
            if len(value.keys()) > 0:
                return "not-empty-object"
        if isinstance(value, list):
            if len(value) > 0:
                return "not-empty-array"
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


def replace_types_by_key(data, target_key):
    def replacement_value(value):
        if isinstance(value, int):
            return "number"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
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


def replace_values_by_key_and_value(data, target_key, target_value):
    def replace_recursive(data):
        if isinstance(data, dict):
            return {key: replace_recursive('accepted-value' if key in target_key and value in target_value else value)
                    for key, value in data.items()}
        elif isinstance(data, list):
            return [replace_recursive(item) for item in data]
        else:
            return data

    return replace_recursive(data)


def wipe_values_by_key(data, target_key):
    def replace_recursive(data):
        if isinstance(data, dict):
            return {key: replace_recursive('accepted-value' if key in target_key else value)
                    for key, value in data.items()}
        elif isinstance(data, list):
            return [replace_recursive(item) for item in data]
        else:
            return data

    return replace_recursive(data)


def delete_keys_recursively(data, keys_to_delete):
    if isinstance(data, dict):
        for key in list(data.keys()):
            if key in keys_to_delete:
                del data[key]
            else:
                delete_keys_recursively(data[key], keys_to_delete)
    elif isinstance(data, list):
        for item in data:
            delete_keys_recursively(item, keys_to_delete)


def normalize_result_pdisks(result):
    result = replace_values_by_key(result, ['AvailableSize',
                                            'TotalSize',
                                            'LogUsedSize',
                                            'LogTotalSize',
                                            'SystemSize',
                                            'SlotSize',
                                            'EnforcedDynamicSlotSize',
                                            ])
    result = replace_values_by_key_and_value(result, ['Status'], ['ACTIVE', 'INACTIVE'])
    return result


def normalize_result_vdisks(result):
    return replace_values_by_key(result, ['AvailableSize',
                                          'IncarnationGuid',
                                          'InstanceGuid',
                                          'WriteThroughput',
                                          'ReadThroughput',
                                          ])


def normalize_result_groups(result):
    return replace_values_by_key(result, ['Available',
                                          'Limit',
                                          ])


def normalize_result_nodes(result):
    result = replace_types_by_key(result, ['ClockSkewUs',
                                           'ClockSkewMinUs',
                                           'ClockSkewMaxUs',
                                           'NetworkUtilization',
                                           'NetworkUtilizationMin',
                                           'NetworkUtilizationMax',
                                           'NetworkWriteThroughput',
                                           'PingTimeUs',
                                           'PingTimeMinUs',
                                           'PingTimeMaxUs',
                                           'ReverseClockSkewUs',
                                           'ReversePingTimeUs',
                                           'Utilization',
                                           'BytesWritten',
                                           'ReceiveThroughput',
                                           'SendThroughput',
                                           'UptimeSeconds',
                                           'Usage',
                                           'TotalSessions',
                                           ])
    return replace_values_by_key(result, ['CpuUsage',
                                          'DiskSpaceUsage',
                                          'Address',
                                          'Port',
                                          'port',
                                          'host',
                                          'Host',
                                          'PeerName',
                                          'LoadAverage',
                                          'MemoryStats',
                                          'MemoryTotal',
                                          'MemoryLimit',
                                          'NumberOfCpus',
                                          'CoresUsed',
                                          'CoresTotal',
                                          'CreateTime',
                                          'MaxDiskUsage',
                                          'Roles',
                                          'ConnectTime',
                                          'Connections',
                                          ])


def normalize_result_info(result):
    return replace_values_by_key(result, ['ChangeTime',
                                          'StartTime',
                                          'ResponseTime',
                                          'ResponseDuration',
                                          'ProcessDuration',
                                          ])


def normalize_result_schema(result):
    return replace_values_by_key(result, ['CreateStep',
                                          'ACL',
                                          'EffectiveACL',
                                          'CreateTxId',
                                          'PathId',
                                          ])


def normalize_result_cluster(result):
    return replace_values_by_key(result, ['MapVersions',
                                          'Versions',
                                          'DataCenters',
                                          'Metrics',
                                          'StorageTotal',
                                          'StorageUsed',
                                          'ROT',
                                          ])


def normalize_result_healthcheck(result):
    result = replace_values_by_key_and_value(result, ['self_check_result'], ['GOOD', 'DEGRADED', 'MAINTENANCE_REQUIRED', 'EMERGENCY'])
    delete_keys_recursively(result, ['issue_log'])
    return result


def normalize_result(result):
    delete_keys_recursively(result, ['Version',
                                     'MemoryUsed',
                                     'WriteThroughput',
                                     'ReadThroughput',
                                     'Read',
                                     'Write',
                                     ])
    result = wipe_values_by_key(result, ['LatencyGetFast',
                                         'LatencyPutTabletLog',
                                         'LatencyPutUserData'
                                         ])
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
    result = get_viewer_db_normalized("/viewer/nodes", {
        'fields_required': 'all'
    })
    return result


def test_storage_groups():
    return normalize_result(get_viewer("/storage/groups", {
        'fields_required': 'all'
    }))


def test_viewer_sysinfo():
    result = get_viewer_normalized("/viewer/sysinfo")
    return result


def test_viewer_vdiskinfo():
    return get_viewer_normalized("/viewer/vdiskinfo")


def test_viewer_pdiskinfo():
    return get_viewer_normalized("/viewer/pdiskinfo")


def test_viewer_bsgroupinfo():
    return get_viewer_normalized("/viewer/bsgroupinfo")


def test_viewer_tabletinfo():
    result = {}
    result['totals'] = get_viewer_db_normalized("/viewer/tabletinfo", {
        'group': 'Type',
        'enums': 'true',
    })
    for name in databases:
        result['totals'][name]['TabletStateInfo'].sort(key=lambda x: x['Type'])
    result['detailed'] = get_viewer_db_normalized("/viewer/tabletinfo")
    for name in databases:
        result['detailed'][name]['TabletStateInfo'].sort(key=lambda x: x['TabletId'])
    return result


def test_viewer_describe():
    result = {}
    for name in databases:
        result[name] = get_viewer_normalized("/viewer/describe", {
            'database': name,
            'path': name
            })
    return result


def test_viewer_cluster():
    return get_viewer_normalized("/viewer/cluster")


def test_viewer_tenantinfo():
    return get_viewer_normalized("/viewer/tenantinfo")


def test_viewer_tenantinfo_db():
    return get_viewer_db_normalized("/viewer/tenantinfo")


def test_viewer_healthcheck():
    result = get_viewer_db_normalized("/viewer/healthcheck")
    result = normalize_result_healthcheck(result)
    return result


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


def test_viewer_query_issue_13945():
    return get_viewer_db("/viewer/query", {
        'query': 'SELECT AsList();',
        'schema': 'multi'
    })


def test_pqrb_tablet():
    response_create_topic = call_viewer("/viewer/query", {
        'database': dedicated_db,
        'query': 'CREATE TOPIC topic1(CONSUMER consumer1)',
        'schema': 'multi'
    })
    response_tablet_info = call_viewer("/viewer/tabletinfo", {
        'database': dedicated_db,
        'path': dedicated_db + '/topic1',
        'enums': 'true'
    })
    result = {
        'response_create_topic': response_create_topic,
        'response_tablet_info': response_tablet_info,
    }
    return replace_values_by_key(result, ['version',
                                          'ResponseTime',
                                          'ChangeTime',
                                          'HiveId',
                                          'NodeId',
                                          'TabletId',
                                          'PathId',
                                          'SchemeShard'
                                          ])


def test_viewer_nodes_issue_14992():
    response_group_by = get_viewer_normalized("/viewer/nodes", {
        'group': 'Uptime'
    })
    response_group = get_viewer_normalized("/viewer/nodes", {
        'filter_group_by': 'Uptime',
        'filter_group' : response_group_by['NodeGroups'][0]['GroupName'],
    })
    result = {
        'response_group_by': response_group_by,
        'response_group': response_group,
    }
    return result


def test_topic_data():
    grpc_port = cluster.nodes[1].grpc_port

    call_viewer("/viewer/query", {
        'database': dedicated_db,
        'query': 'CREATE TOPIC topic1',
        'schema': 'multi'
    })

    endpoint = "localhost:{}".format(grpc_port)
    driver = ydb.Driver(endpoint=endpoint, database=dedicated_db, oauth=None)
    driver.wait(10, fail_fast=True)
    driver.topic_client.create_topic('topic2', min_active_partitions=1, max_active_partitions=1)

    def write(writer, message_pattern, close=True):
        writer.write(["{}-{}".format(message_pattern, i) for i in range(10)])
        writer.flush()
        if close:
            writer.close()

    writer = driver.topic_client.writer('topic2', producer_id="12345")
    write(writer, "message", False)

    # Also write one messagewith metadata
    message_w_meta = PublicMessage(data="message_with_meta", metadata_items={"key1": "value1", "key2": "value2"})
    writer.write(message_w_meta)
    writer.close()

    writer_compressed = driver.topic_client.writer('topic2', producer_id="12345", codec=2)
    write(writer_compressed, "compressed-message")

    response = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': '{}/topic2'.format(dedicated_db),
        'partition': '0',
        'offset': '0',
        'limit': '5'
    })

    response_w_meta = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': '{}/topic2'.format(dedicated_db),
        'partition': '0',
        'offset': '10',
        'limit': '1'
    })
    response_compressed = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': '{}/topic2'.format(dedicated_db),
        'partition': '0',
        'offset': '11',
        'limit': '5'
    })

    response_last = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': '{}/topic2'.format(dedicated_db),
        'partition': '0',
        'offset': '20',
        'limit': '5'
    })

    def strip_non_canonized(resp):
        for message in resp["Messages"]:
            assert int(message.get("CreateTimestamp", "0")) != 0
            assert int(message.get("WriteTimestamp", "0")) != 0
            assert int(message.get("TimestampDiff", None)) >= 0
            assert message.get("ProducerId", None) is not None
            del message["CreateTimestamp"]
            del message["WriteTimestamp"]
            del message["TimestampDiff"]
            del message["ProducerId"]
        return resp

    result = {
        'response_read': strip_non_canonized(response),
        'response_metadata': strip_non_canonized(response_w_meta),
        'response_compressed': strip_non_canonized(response_compressed),
        'response_not_truncated': strip_non_canonized(response_last)
    }
    return result
