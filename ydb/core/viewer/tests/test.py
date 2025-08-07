# -*- coding: utf-8 -*-

import logging
import json
import ydb
from ydb._topic_writer.topic_writer import PublicMessage
from ydb.tests.library.harness.kikimr_runner import KiKiMR

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
import requests
from urllib.parse import urlencode
import time
import re


cluster = KiKiMR(KikimrConfigGenerator(extra_feature_flags={
    'enable_alter_database_create_hive_first': True,
    'enable_topic_transfer': True,
    'enable_script_execution_operations': True,
    }))
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


def call_viewer_api_get(url, headers=None):
    port = cluster.nodes[1].mon_port
    return requests.get("http://localhost:%s%s" % (port, url), headers=headers)


def call_viewer_api_post(url, body=None, headers=None):
    port = cluster.nodes[1].mon_port
    return requests.post("http://localhost:%s%s" % (port, url), json=body, headers=headers)


def call_viewer_api_delete(url, headers=None):
    port = cluster.nodes[1].mon_port
    return requests.delete("http://localhost:%s%s" % (port, url), headers=headers)


def get_result(result):
    if result.status_code == 200 and result.headers.get("Content-Type") == "application/json":
        return result.json()
    return {"status_code": result.status_code, "text": result.text}


def call_viewer(url, params=None, headers=None):
    if params is None:
        params = {}
    return get_result(call_viewer_api_get(url + '?' + urlencode(params), headers))


def call_viewer_post(url, params=None, body=None, headers=None):
    if params is None:
        params = {}
    return get_result(call_viewer_api_post(url + '?' + urlencode(params), body, headers))


def call_viewer_delete(url, params=None, headers=None):
    if params is None:
        params = {}
    return get_result(call_viewer_api_delete(url + '?' + urlencode(params), headers))


def call_viewer_db(url, params=None):
    if params is None:
        params = {}
    result = {}
    for name in databases:
        params["database"] = name
        result[name] = call_viewer(url, params)
    return result


def get_viewer_db(url, params=None):
    if params is None:
        params = {}
    return call_viewer_db(url, params)


def call_viewer_db_not_domain(url, params=None):
    if params is None:
        params = {}
    result = {}
    for name in databases:
        if name == domain_name:
            continue
        params["database"] = name
        result[name] = call_viewer(url, params)
    return result


def get_viewer_db_not_domain(url, params=None):
    if params is None:
        params = {}
    return call_viewer_db_not_domain(url, params)


def get_viewer(url, params=None):
    if params is None:
        params = {}
    return call_viewer(url, params)


def post_viewer(url, params=None, body=None):
    if params is None:
        params = {}
    return call_viewer_post(url, params, body)


def delete_viewer(url, params=None):
    if params is None:
        params = {}
    return call_viewer_delete(url, params)


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
                'query': 'create table table1(id int64, name text, primary key(id))',
                'schema': 'multi'
            })
            call_viewer("/viewer/query", {
                'database': database,
                'query': 'insert into table1(id, name) values(1, "one")',
                'schema': 'multi'
            })
            call_viewer("/viewer/query", {
                'database': database,
                'query': 'insert into table1(id, name) values(2, "two")',
                'schema': 'multi'
            })
            call_viewer("/viewer/query", {
                'database': database,
                'query': 'insert into table1(id, name) values(3, "three")',
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
                                          'NumberOfCpus',
                                          'CoresUsed',
                                          'CoresTotal',
                                          'RealNumberOfCpus',
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


def normalize_result_replication(result):
    result = replace_values_by_key(result, ['connection_string',
                                            'endpoint',
                                            'plan_step',
                                            'tx_id'])
    delete_keys_recursively(result, ['issue_log'])
    return result


def normalize_result(result):
    delete_keys_recursively(result, ['Version',
                                     'MemoryUsed',
                                     'MemoryLimit',
                                     'MemoryTotal',
                                     'WriteThroughput',
                                     'ReadThroughput',
                                     'Read',
                                     'Write',
                                     'size_bytes',
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
    result = normalize_result_replication(result)
    return result


def get_viewer_normalized(url, params=None):
    return normalize_result(get_viewer(url, params))


def get_viewer_db_normalized(url, params=None):
    return normalize_result(get_viewer_db(url, params))


def test_viewer_nodes():
    result = get_viewer_db_normalized("/viewer/nodes", {
    })
    return result


def test_viewer_nodes_all():
    result = get_viewer_db_normalized("/viewer/nodes", {
        'fields_required': 'all'
    })
    return result


def test_viewer_storage_nodes():
    result = get_viewer_db_normalized("/viewer/nodes", {
        'type': 'storage',
    })
    return result


def test_viewer_storage_nodes_all():
    result = get_viewer_db_normalized("/viewer/nodes", {
        'type': 'storage',
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


def test_viewer_acl_write():
    return [
        post_viewer("/viewer/acl", {
            'database': dedicated_db,
            'path': dedicated_db
        }, {
            'AddAccess': [{
                'Subject': 'user1',
                'AccessRights': ['Read']
            }]
        }),
        get_viewer("/viewer/acl", {
            'database': dedicated_db,
            'path': dedicated_db
        }),
        post_viewer("/viewer/acl", {
            'database': dedicated_db,
            'path': dedicated_db
        }, {
            'RemoveAccess': [{
                'Subject': 'user1',
                'AccessRights': ['Read']
            }]
        }),
        get_viewer("/viewer/acl", {
            'database': dedicated_db,
            'path': dedicated_db
        }),
        post_viewer("/viewer/acl", {
            'database': dedicated_db,
            'path': dedicated_db
        }, {
            'ChangeOwnership': {
                'Subject': 'user1',
            }
        }),
        get_viewer("/viewer/acl", {
            'database': dedicated_db,
            'path': dedicated_db
        })]


def test_viewer_autocomplete():
    return get_viewer_db("/viewer/autocomplete", {'prefix': ''})


def test_viewer_check_access():
    db = cluster.domain_name
    return get_viewer_db("/viewer/check_access", {'path': db, 'permissions': 'read'})


def test_viewer_query():
    return get_viewer_db("/viewer/query", {'query': 'select 7*6', 'schema': 'multi'})


def test_viewer_query_from_table():
    return get_viewer_db_not_domain("/viewer/query", {'query': 'select * from table1', 'schema': 'multi'})


def test_viewer_query_from_table_different_schemas():
    result = {}
    for schema in ['classic', 'multi', 'modern', 'ydb', 'ydb2']:
        result[schema] = get_viewer("/viewer/query", {
            'database': dedicated_db,
            'query': 'select * from table1',
            'schema': schema
            })
    return result


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


def test_operations_list():
    return get_viewer_normalized("/operation/list", {
        'database': dedicated_db,
        'kind': 'import/s3'
    })


def test_operations_list_page():
    return get_viewer_normalized("/operation/list", {
        'database': dedicated_db,
        'kind': 'import/s3',
        'offset': 50,
        'limit': 50
    })


def test_operations_list_page_bad():
    return get_viewer_normalized("/operation/list", {
        'database': dedicated_db,
        'kind': 'import/s3',
        'offset': 10,
        'limit': 50
    })


def test_scheme_directory():

    result = {}
    result["1-get"] = get_viewer_normalized("/scheme/directory", {
        'database': dedicated_db,
        'path': dedicated_db
        })
    logging.info("Result 1: {}".format(result["1-get"]))

    result["2-post"] = post_viewer("/scheme/directory", {
        'database': dedicated_db,
        'path': dedicated_db + '/test_dir'
        })
    result["3-get"] = get_viewer_normalized("/scheme/directory", {
        'database': dedicated_db,
        'path': dedicated_db
        })
    result["4-delete"] = delete_viewer("/scheme/directory", {
        'database': dedicated_db,
        'path': dedicated_db + '/test_dir'
        })
    result["5-get"] = get_viewer_normalized("/scheme/directory", {
        'database': dedicated_db,
        'path': dedicated_db
        })
    return result


def test_topic_data():
    grpc_port = cluster.nodes[1].grpc_port

    call_viewer("/viewer/query", {
        'database': dedicated_db,
        'query': 'CREATE TOPIC topic2',
        'schema': 'multi'
    })

    endpoint = "localhost:{}".format(grpc_port)
    driver = ydb.Driver(endpoint=endpoint, database=dedicated_db, oauth=None)
    driver.wait(10, fail_fast=True)

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
    writer_compressed.close()

    topic_path = '{}/topic2'.format(dedicated_db)

    response = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '0',
        'limit': '5'
    })

    response_w_meta = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '10',
        'limit': '1'
    })
    response_compressed = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '11',
        'limit': '5'
    })

    response_last = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '20',
        'limit': '5'
    })

    response_short_msg = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '20',
        'limit': '1',
        'message_size_limit': '5'
    })

    response_no_part = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'offset': '20'
    })

    response_both_offset_and_ts = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '20',
        'read_timestamp': '20'
    })

    response_cut_by_last_offset = call_viewer("/viewer/topic_data", {
        'database': dedicated_db,
        'path': topic_path,
        'partition': '0',
        'offset': '0',
        'last_offset': '3',
        'limit': '10'
    })

    def replace_values(resp):
        res = replace_values_by_key(resp, ['CreateTimestamp',
                                           'WriteTimestamp',
                                           'ProducerId',
                                           ])
        res = replace_types_by_key(res, ['TimestampDiff'])
        logging.info(res)
        return res

    result = {
        'response_read': replace_values(response),
        'response_metadata': replace_values(response_w_meta),
        'response_compressed': replace_values(response_compressed),
        'response_not_truncated': replace_values(response_last),
        'no_partition': response_no_part,
        'both_offset_and_ts': response_both_offset_and_ts,
        'response_truncated': replace_values(response_short_msg),
        'response_last_offset': replace_values(response_cut_by_last_offset),
    }
    return result


def test_transfer_describe():
    grpc_port = cluster.nodes[1].grpc_port
    endpoint = "grpc://localhost:{}/?database={}".format(grpc_port, dedicated_db)

    call_viewer("/viewer/query", {
        'database': dedicated_db,
        'query': 'CREATE ASYNC REPLICATION `TestAsyncReplication` FOR `TableNotExists` AS `TargetAsyncReplicationTable` WITH (CONNECTION_STRING = "{}")'.format(endpoint),
        'schema': 'multi'
    })

    result = get_viewer_normalized("/viewer/describe_replication", {
        'database': dedicated_db,
        'path': '{}/TestAsyncReplication'.format(dedicated_db),
        'include_stats': 'true',
        'enums': 'true'
    })

    return result


def normalize_result_query_long(result):
    """Normalize operation_id and execution_id for long query tests"""
    result = replace_values_by_key(result, ['operation_id',
                                            'execution_id',
                                            'ResponseTime',
                                            'ResponseDuration',
                                            'ProcessDuration',
                                            'id',
                                            ])
    return result


def test_viewer_query_long():
    """Test execute-long-query and fetch-long-query functionality"""
    # First, execute a long query that will return operation_id and execution_id
    response_execute = call_viewer("/viewer/query", {
        'database': dedicated_db,
        'action': 'execute-long-query',
        'query': 'SELECT * FROM table1 LIMIT 5;',
        'schema': 'multi'
    })

    # Normalize the response to hide dynamic values
    response_execute_normalized = normalize_result_query_long(response_execute)

    # If we got operation_id and execution_id, test fetching with both
    result = {
        'execute_response': response_execute_normalized
    }

    if 'operation_id' in response_execute and 'execution_id' in response_execute:
        operation_id = response_execute['operation_id']
        execution_id = response_execute['execution_id']

        # Test fetch with operation_id
        response_fetch_by_operation = call_viewer("/viewer/query", {
            'database': dedicated_db,
            'action': 'fetch-long-query',
            'operation_id': operation_id,
            'schema': 'multi'
        })
        result['fetch_by_operation_id'] = normalize_result_query_long(response_fetch_by_operation)

        # Test fetch with execution_id
        response_fetch_by_execution = call_viewer("/viewer/query", {
            'database': dedicated_db,
            'action': 'fetch-long-query',
            'execution_id': execution_id,
            'schema': 'multi'
        })
        result['fetch_by_execution_id'] = normalize_result_query_long(response_fetch_by_execution)

        # Test error case - missing both operation_id and execution_id
        response_fetch_error = call_viewer("/viewer/query", {
            'database': dedicated_db,
            'action': 'fetch-long-query',
            'schema': 'multi'
        })
        result['fetch_error_no_id'] = response_fetch_error

        # Test error case - invalid operation_id
        response_fetch_invalid = call_viewer("/viewer/query", {
            'database': dedicated_db,
            'action': 'fetch-long-query',
            'operation_id': 'invalid-operation-id',
            'schema': 'multi'
        })
        result['fetch_invalid_operation_id'] = response_fetch_invalid

    return result


def call_viewer_multipart_parsed(path, params=None):
    """Call viewer endpoint expecting multipart response and parse JSON parts"""
    if params is None:
        params = {}

    # Make raw HTTP request to get multipart response
    port = cluster.nodes[1].mon_port
    headers = {'Accept': 'multipart/x-mixed-replace'}
    response = requests.get("http://localhost:%s%s?%s" % (port, path, urlencode(params)), headers=headers)

    if response.status_code != 200:
        return {"status_code": response.status_code, "text": response.text}

    content_type = response.headers.get('content-type', '')

    # If it's not a multipart response, try to parse as JSON
    if not content_type.startswith('multipart/'):
        if content_type == 'application/json':
            try:
                return response.json()
            except (ValueError, json.JSONDecodeError):
                return {"status_code": response.status_code, "text": response.text}
        else:
            return {"status_code": response.status_code, "text": response.text}

    # Parse multipart content
    raw_text = response.text

    # Extract boundary from content-type header
    boundary_match = re.search(r'boundary=([^;]+)', content_type)
    if not boundary_match:
        return {"error": "No boundary found in multipart response"}

    boundary = boundary_match.group(1).strip('"')

    # Split content by boundary
    parts = raw_text.split('--' + boundary)
    json_parts = []

    for part in parts:
        if not part.strip() or part.strip() == '--':
            continue

        # Split headers and body
        if '\r\n\r\n' in part:
            headers_section, body = part.split('\r\n\r\n', 1)
        elif '\n\n' in part:
            headers_section, body = part.split('\n\n', 1)
        else:
            continue

        # Check if this part contains JSON
        if 'application/json' in headers_section:
            try:
                # Clean up the body and parse JSON
                body = body.strip()
                if body.endswith('\r\n--'):
                    body = body[:-4]
                elif body.endswith('\n--'):
                    body = body[:-3]

                json_data = json.loads(body)
                json_parts.append(json_data)
            except json.JSONDecodeError:
                # Skip non-JSON parts
                continue

    # Return structured response with parsed JSON parts
    if json_parts:
        return {"multipart_parts": json_parts, "status_code": 200}
    else:
        # No JSON parts found, return raw response for canonization
        return {"status_code": 200, "raw_text": raw_text}


def normalize_multipart_response(response):
    """Helper function to normalize multipart responses"""
    if 'multipart_parts' in response:
        normalized_parts = []
        for part in response['multipart_parts']:
            normalized_part = normalize_result_query_long(part)
            normalized_parts.append(normalized_part)
        return {"multipart_parts": normalized_parts}
    return response


def test_viewer_query_long_multipart():
    """Test execute-long-query with multipart streaming (schema=multipart)"""

    # Test successful long query with multipart streaming
    response_execute_stream = call_viewer_multipart_parsed("/viewer/query", {
        'database': dedicated_db,
        'action': 'execute-long-query',
        'query': 'SELECT * FROM table1 LIMIT 3;',
        'schema': 'multipart'
    })

    result = {}

    # Apply normalization to multipart responses
    result['execute_stream_response'] = normalize_multipart_response(response_execute_stream)

    # Test error case with multipart streaming - invalid operation_id
    response_fetch_invalid_stream = call_viewer_multipart_parsed("/viewer/query", {
        'database': dedicated_db,
        'action': 'fetch-long-query',
        'operation_id': 'invalid-operation-id',
        'schema': 'multipart'
    })

    result['fetch_invalid_stream_response'] = normalize_multipart_response(response_fetch_invalid_stream)

    # Test error case - missing operation_id/execution_id with multipart
    response_fetch_error_stream = call_viewer_multipart_parsed("/viewer/query", {
        'database': dedicated_db,
        'action': 'fetch-long-query',
        'schema': 'multipart'
    })

    result['fetch_error_stream_response'] = normalize_multipart_response(response_fetch_error_stream)

    return result
