# -*- coding: utf-8 -*-

import logging
import json
import pytest
import ydb
from ydb._topic_writer.topic_writer import PublicMessage
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
import requests
from urllib.parse import urlencode
import time
import re


class TestViewer(object):
    @pytest.fixture(autouse=True, scope='class')
    @classmethod
    def cluster_fixture(cls):
        config = KikimrConfigGenerator(extra_feature_flags={
            'enable_alter_database_create_hive_first': True,
            'enable_topic_transfer': True,
            'enable_script_execution_operations': True,
            'enable_resource_pools': True,  # just for canonized data - .metadata table
            },
            enable_static_auth=True)
        config.yaml_config['domains_config']['security_config']['enforce_user_token_requirement'] = False
        config.yaml_config['domains_config']['security_config']['enforce_user_token_check_requirement'] = True
        config.yaml_config['domains_config']['security_config']['database_allowed_sids'] = ['database']
        config.yaml_config['domains_config']['security_config']['viewer_allowed_sids'] = ['viewer']
        config.yaml_config['domains_config']['security_config']['monitoring_allowed_sids'] = ['monitoring', 'root']
        config.yaml_config['domains_config']['security_config']['administration_allowed_sids'] = ['root']
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        cls.root_session_id = cls.login_user({'user': 'root', 'password': '1234'}).cookies.get('ydb_session_id')
        cls.root_token = 'Login ' + cls.root_session_id
        cls.database_session_id = ''
        cls.viewer_session_id = ''
        cls.monitoring_session_id = ''
        cls.domain_name = '/' + cls.cluster.domain_name
        cls.dedicated_db = cls.domain_name + "/dedicated_db"
        cls.shared_db = cls.domain_name + "/shared_db"
        cls.serverless_db = cls.domain_name + "/serverless_db"
        cls.cluster.create_database(cls.dedicated_db,
                                    storage_pool_units_count={
                                        'hdd': 1
                                    },
                                    token=cls.root_token)
        cls.cluster.register_and_start_slots(cls.dedicated_db, count=1)
        cls.cluster.wait_tenant_up(cls.dedicated_db, token=cls.root_token)
        cls.cluster.create_hostel_database(cls.shared_db,
                                           storage_pool_units_count={
                                               'hdd': 1
                                           },
                                           token=cls.root_token)
        cls.cluster.register_and_start_slots(cls.shared_db, count=1)
        cls.cluster.wait_tenant_up(cls.shared_db, token=cls.root_token)
        cls.cluster.create_serverless_database(cls.serverless_db, cls.shared_db, token=cls.root_token)
        cls.cluster.wait_tenant_up(cls.serverless_db, token=cls.root_token)
        cls.databases = [cls.domain_name, cls.dedicated_db, cls.shared_db, cls.serverless_db]
        cls.databases_and_no_database = ['no-database', cls.domain_name, cls.dedicated_db, cls.shared_db, cls.serverless_db]
        cls.default_headers = {
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        }
        cls.wait_for_cluster_ready()
        yield
        cls.cluster.stop()

    @classmethod
    def login_user(cls, request):
        return requests.post("http://localhost:%s/login" % cls.cluster.nodes[1].mon_port, json=request)

    @classmethod
    def call_viewer_api_get(cls, url, headers=None):
        if headers is None:
            headers = cls.default_headers
        port = cls.cluster.nodes[1].mon_port
        return requests.get("http://localhost:%s%s" % (port, url), headers=headers)

    @classmethod
    def call_viewer_api_post(cls, url, body=None, headers=None):
        if headers is None:
            headers = cls.default_headers
        port = cls.cluster.nodes[1].mon_port
        return requests.post("http://localhost:%s%s" % (port, url), json=body, headers=headers)

    @classmethod
    def call_viewer_api_delete(cls, url, headers=None):
        if headers is None:
            headers = cls.default_headers
        port = cls.cluster.nodes[1].mon_port
        return requests.delete("http://localhost:%s%s" % (port, url), headers=headers)

    @classmethod
    def get_result(cls, result):
        if result.status_code == 200 and result.headers.get("Content-Type") == "application/json":
            return result.json()
        return {"status_code": result.status_code, "text": result.text}

    @classmethod
    def call_viewer(cls, url, params=None, headers=None):
        if params is None:
            params = {}
        if headers is None:
            headers = cls.default_headers
        return cls.get_result(cls.call_viewer_api_get(url + '?' + urlencode(params), headers))

    @classmethod
    def call_viewer_post(cls, url, params=None, body=None, headers=None):
        if params is None:
            params = {}
        if headers is None:
            headers = cls.default_headers
        return cls.get_result(cls.call_viewer_api_post(url + '?' + urlencode(params), body, headers))

    @classmethod
    def call_viewer_delete(cls, url, params=None, headers=None):
        if params is None:
            params = {}
        if headers is None:
            headers = cls.default_headers
        return cls.get_result(cls.call_viewer_api_delete(url + '?' + urlencode(params), headers))

    @classmethod
    def call_viewer_db(cls, url, params=None):
        if params is None:
            params = {}
        result = {}
        result["no-database"] = cls.call_viewer(url, params)
        for name in cls.databases:
            params["database"] = name
            result[name] = cls.call_viewer(url, params)
        return result

    @classmethod
    def get_viewer_db(cls, url, params=None):
        if params is None:
            params = {}
        return cls.call_viewer_db(url, params)

    @classmethod
    def call_viewer_db_not_domain(cls, url, params=None):
        if params is None:
            params = {}
        result = {}
        for name in cls.databases:
            if name == cls.domain_name:
                continue
            params["database"] = name
            result[name] = cls.call_viewer(url, params)
        return result

    @classmethod
    def get_viewer_db_not_domain(cls, url, params=None):
        if params is None:
            params = {}
        return cls.call_viewer_db_not_domain(url, params)

    @classmethod
    def get_viewer(cls, url, params=None, headers=None):
        if params is None:
            params = {}
        if headers is None:
            headers = cls.default_headers
        return cls.call_viewer(url, params, headers)

    @classmethod
    def post_viewer(cls, url, params=None, body=None, headers=None):
        if params is None:
            params = {}
        if headers is None:
            headers = cls.default_headers
        return cls.call_viewer_post(url, params, body, headers)

    @classmethod
    def delete_viewer(cls, url, params=None):
        if params is None:
            params = {}
        return cls.call_viewer_delete(url, params)

    @classmethod
    def wait_for_cluster_ready(cls):
        wait_time = 0
        max_wait_time = 300
        for node_id, node in cls.cluster.nodes.items():
            while wait_time < max_wait_time:
                all_good = False
                while True:
                    try:
                        print("Waiting for node %s to be ready" % node_id)
                        result_counter = cls.get_result(requests.get("http://localhost:%s/viewer/simple_counter?max_counter=1&period=1" % node.mon_port, headers=cls.default_headers))
                        if result_counter['status_code'] != 200:
                            break
                        result = cls.get_result(requests.get("http://localhost:%s/viewer/sysinfo?node_id=." % node.mon_port, headers=cls.default_headers))
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
        cls.call_viewer("/viewer/query", {
            'database': cls.domain_name,
            'query': 'create user database password "2345"'
        })
        cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'grant select on `' + cls.dedicated_db + '` to database;'
        })
        cls.call_viewer("/viewer/query", {
            'database': cls.domain_name,
            'query': 'create user viewer password "3456"'
        })
        cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'grant select on `' + cls.dedicated_db + '` to viewer;'
        })
        cls.call_viewer("/viewer/query", {
            'database': cls.domain_name,
            'query': 'create user monitoring password "4567"'
        })
        cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'grant select on `' + cls.dedicated_db + '` to monitoring;'
        })
        cls.database_session_id = cls.login_user({'user': 'database', 'password': '2345'}).cookies.get('ydb_session_id')
        cls.viewer_session_id = cls.login_user({'user': 'viewer', 'password': '3456'}).cookies.get('ydb_session_id')
        cls.monitoring_session_id = cls.login_user({'user': 'monitoring', 'password': '4567'}).cookies.get('ydb_session_id')
        for database in cls.databases:
            if database != cls.domain_name:
                cls.call_viewer("/viewer/query", {
                    'database': database,
                    'query': 'create table table1(id int64, name text, primary key(id))',
                    'schema': 'multi'
                })
                cls.call_viewer("/viewer/query", {
                    'database': database,
                    'query': 'insert into table1(id, name) values(1, "one")',
                    'schema': 'multi'
                })
                cls.call_viewer("/viewer/query", {
                    'database': database,
                    'query': 'insert into table1(id, name) values(2, "two")',
                    'schema': 'multi'
                })
                cls.call_viewer("/viewer/query", {
                    'database': database,
                    'query': 'insert into table1(id, name) values(3, "three")',
                    'schema': 'multi'
                })
        for database in cls.databases:
            while wait_time < max_wait_time:
                all_good = False
                print("Waiting for database %s to be ready" % database)
                while True:
                    result = cls.get_result(requests.get(
                        "http://localhost:%s/viewer/tenantinfo?database=%s" %
                        (cls.cluster.nodes[1].mon_port, database), headers=cls.default_headers))  # force connect between nodes
                    if 'status_code' in result and result['status_code'] != 200:
                        break
                    if 'CoresUsed' not in result['TenantInfo'][0]:
                        break
                    result = cls.get_result(requests.get(
                        "http://localhost:%s/viewer/healthcheck?database=%s" %
                        (cls.cluster.nodes[1].mon_port, database), headers=cls.default_headers))  # force connect between nodes
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
                result = cls.call_viewer("/viewer/query", {
                    'database': cls.domain_name,
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
                result = cls.get_result(requests.get(
                    "http://localhost:%s/storage/groups?fields_required=all" %
                    (cls.cluster.nodes[1].mon_port), headers=cls.default_headers))  # force connect between nodes
                if 'status_code' in result and result['status_code'] != 200:
                    break
                if len(result['StorageGroups']) < 5:
                    break
                result = cls.get_result(requests.get(
                    "http://localhost:%s/viewer/cluster" %
                    (cls.cluster.nodes[1].mon_port), headers=cls.default_headers))  # force connect between nodes
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
        print('Wait for cluster to be ready took %s seconds' % wait_time)

    @classmethod
    def test_whoami_root(cls):
        return cls.get_viewer_normalized("/viewer/whoami")

    @classmethod
    def test_whoami_database(cls):
        return cls.get_viewer_normalized("/viewer/whoami", headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        })

    @classmethod
    def test_whoami_viewer(cls):
        return cls.get_viewer_normalized("/viewer/whoami", headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        })

    @classmethod
    def test_whoami_monitoring(cls):
        return cls.get_viewer_normalized("/viewer/whoami", headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        })

    @classmethod
    def test_counter(cls):
        return cls.get_viewer("/viewer/simple_counter", {'max_counter': 1})

    @classmethod
    def replace_values_by_key(cls, data, target_key):
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

    @classmethod
    def replace_types_by_key(cls, data, target_key):
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

    @classmethod
    def replace_values_by_key_and_value(cls, data, target_key, target_value):
        def replace_recursive(data):
            if isinstance(data, dict):
                return {key: replace_recursive('accepted-value' if key in target_key and value in target_value else value)
                        for key, value in data.items()}
            elif isinstance(data, list):
                return [replace_recursive(item) for item in data]
            else:
                return data

        return replace_recursive(data)

    @classmethod
    def wipe_values_by_key(cls, data, target_key):
        def replace_recursive(data):
            if isinstance(data, dict):
                return {key: replace_recursive('accepted-value' if key in target_key else value)
                        for key, value in data.items()}
            elif isinstance(data, list):
                return [replace_recursive(item) for item in data]
            else:
                return data

        return replace_recursive(data)

    @classmethod
    def delete_keys_recursively(cls, data, keys_to_delete):
        if isinstance(data, dict):
            for key in list(data.keys()):
                if key in keys_to_delete:
                    del data[key]
                else:
                    cls.delete_keys_recursively(data[key], keys_to_delete)
        elif isinstance(data, list):
            for item in data:
                cls.delete_keys_recursively(item, keys_to_delete)

    @classmethod
    def normalize_result(cls, result):
        cls.delete_keys_recursively(result, {'Version',
                                             'MemoryUsed',
                                             'WriteThroughput',
                                             'ReadThroughput',
                                             'Read',
                                             'Write',
                                             'size_bytes',
                                             })
        result = cls.wipe_values_by_key(result, {'LatencyGetFast',
                                                 'LatencyPutTabletLog',
                                                 'LatencyPutUserData',
                                                 })
        replace_with_types = set()
        replace_with_values = set()

        # nodes
        replace_with_types.update({'ClockSkewUs',
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
                                   })

        replace_with_values.update({'CpuUsage',
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
                                    'RealNumberOfCpus',
                                    'CreateTime',
                                    'MaxDiskUsage',
                                    'Roles',
                                    'ConnectTime',
                                    'Connections',
                                    'ResolveHost',
                                    })

        # info
        replace_with_values.update({'ChangeTime',
                                    'StartTime',
                                    'ResponseTime',
                                    'ResponseDuration',
                                    'ProcessDuration',
                                    })

        # schema
        replace_with_values.update({'CreateStep',
                                    'ACL',
                                    'EffectiveACL',
                                    'CreateTxId',
                                    'PathId',
                                    'PublicKeys',
                                    'OriginalUserToken',
                                    'HashesInitParams',
                                    })

        # groups
        replace_with_values.update({'Available',
                                    'Limit',
                                    'MaxPDiskUsage',
                                    'MaxVDiskSlotUsage',
                                    'MaxNormalizedOccupancy',
                                    'MaxVDiskRawUsage',
                                    })

        # pdisks
        replace_with_values.update({'TotalSize',
                                    'LogUsedSize',
                                    'LogTotalSize',
                                    'SystemSize',
                                    'SlotSize',
                                    'SlotCount',
                                    'EnforcedDynamicSlotSize',
                                    'PDiskUsage',
                                    })

        result = cls.replace_values_by_key_and_value(result, {'Status'}, {'ACTIVE', 'INACTIVE'})

        # vdisks
        replace_with_values.update({'AvailableSize',
                                    'AllocatedSize',
                                    'IncarnationGuid',
                                    'InstanceGuid',
                                    'WriteThroughput',
                                    'ReadThroughput',
                                    'StorageSize',
                                    'StorageCount',
                                    'VDiskSlotUsage',
                                    'NormalizedOccupancy',
                                    'VDiskRawUsage',
                                    })

        # cluster
        replace_with_values.update({'MapVersions',
                                    'Versions',
                                    'DataCenters',
                                    'Metrics',
                                    'StorageTotal',
                                    'StorageUsed',
                                    'ROT',
                                    })

        # tablets
        replace_with_values.update({'DataSize',
                                    'IndexSize',
                                    })

        # replication
        replace_with_values.update({'connection_string',
                                    'endpoint',
                                    'plan_step',
                                    'tx_id'})

        result = cls.replace_types_by_key(result, replace_with_types)
        result = cls.replace_values_by_key(result, replace_with_values)
        return result

    @classmethod
    def normalize_result_healthcheck(cls, result):
        result = cls.replace_values_by_key_and_value(result, ['self_check_result'], ['GOOD', 'DEGRADED', 'MAINTENANCE_REQUIRED', 'EMERGENCY'])
        cls.delete_keys_recursively(result, {'issue_log'})
        return result

    @classmethod
    def get_viewer_normalized(cls, url, params=None, headers=None):
        return cls.normalize_result(cls.get_viewer(url, params, headers))

    @classmethod
    def get_viewer_db_normalized(cls, url, params=None):
        return cls.normalize_result(cls.get_viewer_db(url, params))

    @classmethod
    def test_viewer_nodelist(cls):
        result = cls.get_viewer_db_normalized("/viewer/nodelist", {
        })
        return result

    @classmethod
    def test_viewer_nodes(cls):
        result = cls.get_viewer_db_normalized("/viewer/nodes", {
        })
        return result

    @classmethod
    def test_viewer_nodes_all(cls):
        result = cls.get_viewer_db_normalized("/viewer/nodes", {
            'fields_required': 'all'
        })
        return result

    @classmethod
    def test_viewer_storage_nodes_no_database(cls):
        result = cls.get_viewer_normalized("/viewer/nodes", {
            'type': 'storage',
        })
        return result

    @classmethod
    def test_viewer_storage_nodes_no_database_filter_node_id(cls):
        result = cls.get_viewer_normalized("/viewer/nodes", {
            'type': 'storage',
            'node_id': '1',
        })
        return result

    @classmethod
    def test_viewer_storage_nodes(cls):
        result = cls.get_viewer_db_normalized("/viewer/nodes", {
            'type': 'storage',
        })
        return result

    @classmethod
    def test_viewer_storage_nodes_all(cls):
        result = cls.get_viewer_db_normalized("/viewer/nodes", {
            'type': 'storage',
            'fields_required': 'all'
        })
        return result

    @classmethod
    def test_viewer_nodes_group(cls):
        return [
            cls.get_viewer_normalized("/viewer/nodes", {
                'group': 'CapacityAlert'
            }),
            cls.get_viewer_normalized("/viewer/nodes", {
                'filter_group_by': 'CapacityAlert',
                'filter_group': 'GREEN'
            })
        ]

    @classmethod
    def test_storage_groups(cls):
        return cls.normalize_result(cls.get_viewer("/viewer/groups", {
            'fields_required': 'all'
        }))

    @classmethod
    def test_viewer_groups_group_by_pool_name(cls):
        return [
            cls.get_viewer_normalized("/viewer/groups", {
                'group': 'PoolName'
            }),
            cls.get_viewer_normalized("/viewer/groups", {
                'filter_group_by': 'PoolName',
                'filter_group': 'static'
            })
        ]

    @classmethod
    def test_viewer_groups_group_by_capacity_alert(cls):
        return [
            cls.get_viewer_normalized("/viewer/groups", {
                'group': 'CapacityAlert'
            }),
            cls.get_viewer_normalized("/viewer/groups", {
                'filter_group_by': 'CapacityAlert',
                'filter_group': 'GREEN'
            })
        ]

    @classmethod
    def test_viewer_groups_with_invalid_database(cls):
        # Test that the endpoint doesn't crash when provided with an invalid database
        result = cls.call_viewer("/viewer/groups", {
            'database': '/invalid_database_name_that_does_not_exist',
        })
        return result

    @classmethod
    def test_viewer_sysinfo(cls):
        result = cls.get_viewer_normalized("/viewer/sysinfo")
        return result

    @classmethod
    def test_viewer_vdiskinfo(cls):
        return cls.get_viewer_normalized("/viewer/vdiskinfo")

    @classmethod
    def test_viewer_pdiskinfo(cls):
        return cls.get_viewer_normalized("/viewer/pdiskinfo")

    @classmethod
    def test_viewer_bsgroupinfo(cls):
        return cls.get_viewer_normalized("/viewer/bsgroupinfo")

    @classmethod
    def test_viewer_tabletinfo(cls):
        result = {}
        result['totals'] = cls.get_viewer_db_normalized("/viewer/tabletinfo", {
            'group': 'Type',
            'enums': 'true',
        })
        for name in cls.databases_and_no_database:
            result['totals'][name]['TabletStateInfo'].sort(key=lambda x: x['Type'])
        result['detailed'] = cls.get_viewer_db_normalized("/viewer/tabletinfo")
        for name in cls.databases_and_no_database:
            result['detailed'][name]['TabletStateInfo'].sort(key=lambda x: x['TabletId'])
        return result

    @classmethod
    def test_viewer_describe(cls):
        result = {}
        for name in cls.databases:
            result[name] = cls.get_viewer_normalized("/viewer/describe", {
                'database': name,
                'path': name
                })
        return result

    @classmethod
    def test_viewer_cluster(cls):
        return cls.get_viewer_normalized("/viewer/cluster")

    @classmethod
    def test_viewer_tenantinfo(cls):
        return cls.get_viewer_normalized("/viewer/tenantinfo")

    @classmethod
    def test_viewer_tenantinfo_db(cls):
        return cls.get_viewer_db_normalized("/viewer/tenantinfo")

    @classmethod
    def test_viewer_healthcheck(cls):
        result = cls.get_viewer_db_normalized("/viewer/healthcheck")
        result = cls.normalize_result_healthcheck(result)
        return result

    @classmethod
    def test_viewer_acl(cls):
        db = cls.cluster.domain_name
        return cls.get_viewer_db("/viewer/acl", {'path': db})

    @classmethod
    def test_viewer_acl_write(cls):
        return [
            cls.post_viewer("/viewer/acl", {
                'database': cls.dedicated_db,
                'path': cls.dedicated_db
            }, {
                'AddAccess': [{
                    'Subject': 'user1',
                    'AccessRights': ['Read']
                }]
            }),
            cls.get_viewer("/viewer/acl", {
                'database': cls.dedicated_db,
                'path': cls.dedicated_db
            }),
            cls.post_viewer("/viewer/acl", {
                'database': cls.dedicated_db,
                'path': cls.dedicated_db
            }, {
                'RemoveAccess': [{
                    'Subject': 'user1',
                    'AccessRights': ['Read']
                }]
            }),
            cls.get_viewer("/viewer/acl", {
                'database': cls.dedicated_db,
                'path': cls.dedicated_db
            }),
            cls.post_viewer("/viewer/acl", {
                'database': cls.dedicated_db,
                'path': cls.dedicated_db
            }, {
                'ChangeOwnership': {
                    'Subject': 'user1',
                }
            }),
            cls.get_viewer("/viewer/acl", {
                'database': cls.dedicated_db,
                'path': cls.dedicated_db
            })]

    @classmethod
    def test_viewer_autocomplete(cls):
        return cls.get_viewer_db("/viewer/autocomplete", {'prefix': ''})

    @classmethod
    def test_viewer_check_access(cls):
        db = cls.cluster.domain_name
        return cls.get_viewer_db("/viewer/check_access", {'path': db, 'permissions': 'read'})

    @classmethod
    def test_viewer_query(cls):
        return cls.get_viewer_db("/viewer/query", {'query': 'select 7*6', 'schema': 'multi'})

    @classmethod
    def test_viewer_query_from_table(cls):
        return cls.get_viewer_db_not_domain("/viewer/query", {'query': 'select * from table1', 'schema': 'multi'})

    @classmethod
    def test_viewer_query_from_table_different_schemas(cls):
        result = {}
        for schema in ['classic', 'multi', 'modern', 'ydb', 'ydb2']:
            result[schema] = cls.get_viewer("/viewer/query", {
                'database': cls.dedicated_db,
                'query': 'select * from table1',
                'schema': schema
                })
        return result

    @classmethod
    def test_viewer_query_issue_13757(cls):
        return cls.get_viewer_db("/viewer/query", {
            'query': 'SELECT CAST(<|one:"8912", two:42|> AS Struct<two:Utf8, three:Date?>);',
            'schema': 'multi'
        })

    @classmethod
    def test_viewer_query_issue_13945(cls):
        return cls.get_viewer_db("/viewer/query", {
            'query': 'SELECT AsList();',
            'schema': 'multi'
        })

    @classmethod
    def test_pqrb_tablet(cls):
        response_create_topic = cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'CREATE TOPIC topic1(CONSUMER consumer1)',
            'schema': 'multi'
        })

        response_tablet_info = cls.call_viewer("/viewer/tabletinfo", {
            'database': cls.dedicated_db,
            'path': cls.dedicated_db + '/topic1',
            'enums': 'true'
        })
        result = {
            'response_create_topic': response_create_topic,
            'response_tablet_info': response_tablet_info,
        }
        return cls.replace_values_by_key(result, ['version',
                                                  'ResponseTime',
                                                  'ChangeTime',
                                                  'HiveId',
                                                  'NodeId',
                                                  'TabletId',
                                                  'PathId',
                                                  'SchemeShard',
                                                  ])

    @classmethod
    def test_viewer_nodes_issue_14992(cls):
        response_group_by = cls.get_viewer_normalized("/viewer/nodes", {
            'group': 'Uptime'
        })
        response_group = cls.get_viewer_normalized("/viewer/nodes", {
            'filter_group_by': 'Uptime',
            'filter_group' : response_group_by['NodeGroups'][0]['GroupName'],
        })
        result = {
            'response_group_by': response_group_by,
            'response_group': response_group,
        }
        return result

    @classmethod
    def test_operations_list(cls):
        return cls.get_viewer_normalized("/operation/list", {
            'database': cls.dedicated_db,
            'kind': 'import/s3'
        })

    @classmethod
    def test_operations_list_page(cls):
        return cls.get_viewer_normalized("/operation/list", {
            'database': cls.dedicated_db,
            'kind': 'import/s3',
            'offset': 50,
            'limit': 50
        })

    @classmethod
    def test_operations_list_page_bad(cls):
        return cls.get_viewer_normalized("/operation/list", {
            'database': cls.dedicated_db,
            'kind': 'import/s3',
            'offset': 10,
            'limit': 50
        })

    @classmethod
    def test_scheme_directory(cls):
        result = {}
        result["1-get"] = cls.get_viewer_normalized("/scheme/directory", {
            'database': cls.dedicated_db,
            'path': cls.dedicated_db
            })
        result["2-post"] = cls.post_viewer("/scheme/directory", {
            'database': cls.dedicated_db,
            'path': cls.dedicated_db + '/test_dir'
            })
        result["3-get"] = cls.get_viewer_normalized("/scheme/directory", {
            'database': cls.dedicated_db,
            'path': cls.dedicated_db
            })
        result["4-delete"] = cls.delete_viewer("/scheme/directory", {
            'database': cls.dedicated_db,
            'path': cls.dedicated_db + '/test_dir'
            })
        result["5-get"] = cls.get_viewer_normalized("/scheme/directory", {
            'database': cls.dedicated_db,
            'path': cls.dedicated_db
            })
        return result

    @classmethod
    def test_topic_data(cls):
        grpc_port = cls.cluster.nodes[1].grpc_port

        cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'CREATE TOPIC topic2',
            'schema': 'multi'
        })

        endpoint = "localhost:{}".format(grpc_port)
        driver = ydb.Driver(endpoint=endpoint, database=cls.dedicated_db, oauth=None)
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

        topic_path = '{}/topic2'.format(cls.dedicated_db)

        response = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '0',
            'limit': '5'
        })

        response_w_meta = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '10',
            'limit': '1'
        })
        response_compressed = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '11',
            'limit': '5'
        })

        response_last = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '20',
            'limit': '5'
        })

        response_short_msg = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '20',
            'limit': '1',
            'message_size_limit': '5'
        })

        response_no_part = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'offset': '20'
        })

        response_both_offset_and_ts = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '20',
            'read_timestamp': '20'
        })

        response_cut_by_last_offset = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '0',
            'last_offset': '3',
            'limit': '10'
        })

        def replace_values(resp):
            res = cls.replace_values_by_key(resp, ['CreateTimestamp',
                                                   'WriteTimestamp',
                                                   'ProducerId',
                                                   ])
            res = cls.replace_types_by_key(res, ['TimestampDiff'])
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

    @classmethod
    def test_topic_data_cdc(cls):
        cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'create table table_test_topic_data_cdc(id int64, name text, primary key(id))',
            'schema': 'multi'
        })

        alter_response = cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': "alter table table_test_topic_data_cdc add changefeed updates_feed WITH (FORMAT = 'JSON', MODE = 'UPDATES', INITIAL_SCAN = TRUE)"
        })

        insert_response = None
        for i in range(3):
            insert_response = cls.call_viewer("/viewer/query", {
                'database': cls.dedicated_db,
                'query': 'insert into table_test_topic_data_cdc(id, name) values(11, "elleven")',
                'schema': 'multi'
            })
            if 'error' in insert_response and insert_response['error']['issue_code'] == 2034:
                continue
            else:
                break

        update_response = cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'query': "update table_test_topic_data_cdc set name = 'ONE' where id = 1",
            'schema': 'multi'
        })

        topic_path = '{}/table_test_topic_data_cdc/updates_feed'.format(cls.dedicated_db)
        data_response = cls.call_viewer("/viewer/topic_data", {
            'database': cls.dedicated_db,
            'path': topic_path,
            'partition': '0',
            'offset': '0',
            'limit': '3'
        })

        data_response = cls.replace_values_by_key(
            data_response, ['CreateTimestamp', 'WriteTimestamp', 'ProducerId', ]
        )
        data_response = cls.replace_types_by_key(data_response, ['TimestampDiff'])

        final_result = {"alter" : alter_response, "insert" : insert_response, "update" : update_response, "data" : data_response}
        logging.info("Results: {}".format(final_result))
        return final_result

    @classmethod
    def test_async_replication_describe(cls):
        grpc_port = cls.cluster.nodes[1].grpc_port
        endpoint = "grpc://localhost:{}/?database={}".format(grpc_port, cls.dedicated_db)

        create_result = cls.get_viewer_normalized("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'CREATE ASYNC REPLICATION `TestAsyncReplication` FOR `TableNotExists` AS `TargetAsyncReplicationTable` WITH (CONNECTION_STRING = "{}")'.format(endpoint),
            'schema': 'multi'
        })

        for i in range(60):
            describe_result = cls.get_viewer_normalized("/viewer/describe_replication", {
                'database': cls.dedicated_db,
                'path': '{}/TestAsyncReplication'.format(cls.dedicated_db),
                'include_stats': 'true',
                'enums': 'true'
            })

            if "error" in describe_result:
                break

            time.sleep(1)

        return {
            'create_result': create_result,
            'describe_result': describe_result
        }

    @classmethod
    def test_transfer_describe(cls):
        topic_result = cls.get_viewer_normalized("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'CREATE TOPIC TestTransferSourceTopic (CONSUMER OurConsumer)',
            'schema': 'multi'
        })

        table_result = cls.get_viewer_normalized("/viewer/query", {
            'database': cls.dedicated_db,
            'query': 'CREATE TABLE TestTransferDestinationTable (id Uint64 NOT NULL, message String, PRIMARY KEY (id) )',
            'schema': 'multi'
        })

        transfer_result = cls.get_viewer_normalized("/viewer/query", {
            'database': cls.dedicated_db,
            'query': '''CREATE TRANSFER `TestTransfer` FROM `TestTransferSourceTopic` TO `TestTransferDestinationTable`
                USING ($x) -> { return [<| id: $x._offset, message: $x._data |>]; }
                WITH (CONSUMER='OurConsumer')''',
            'schema': 'multi'
        })

        for i in range(60):
            describe_result = cls.get_viewer_normalized("/viewer/describe_transfer", {
                'database': cls.dedicated_db,
                'path': '{}/TestTransfer'.format(cls.dedicated_db),
                'include_stats': 'true',
                'enums': 'true'
            })

            if "running" in describe_result:
                break

            time.sleep(1)

        return {
            'topic_result': topic_result,
            'table_result': table_result,
            'transfer_result': transfer_result,
            'describe_result': describe_result
        }

    @classmethod
    def normalize_result_query_long(cls, result):
        """Normalize operation_id and execution_id for long query tests"""
        result = cls.replace_values_by_key(result, ['operation_id',
                                                    'execution_id',
                                                    'ResponseTime',
                                                    'ResponseDuration',
                                                    'ProcessDuration',
                                                    'id',
                                                    ])
        return result

    @classmethod
    def test_viewer_query_long(cls):
        """Test execute-long-query and fetch-long-query functionality"""
        # First, execute a long query that will return operation_id and execution_id
        response_execute = cls.call_viewer("/viewer/query", {
            'database': cls.dedicated_db,
            'action': 'execute-long-query',
            'query': 'SELECT * FROM table1 LIMIT 5;',
            'schema': 'multi'
        })

        # Normalize the response to hide dynamic values
        response_execute_normalized = cls.normalize_result_query_long(response_execute)

        # If we got operation_id and execution_id, test fetching with both
        result = {
            'execute_response': response_execute_normalized
        }

        if 'operation_id' in response_execute and 'execution_id' in response_execute:
            operation_id = response_execute['operation_id']
            execution_id = response_execute['execution_id']

            # Test fetch with operation_id
            response_fetch_by_operation = cls.call_viewer("/viewer/query", {
                'database': cls.dedicated_db,
                'action': 'fetch-long-query',
                'operation_id': operation_id,
                'schema': 'multi'
            })
            result['fetch_by_operation_id'] = cls.normalize_result_query_long(response_fetch_by_operation)

            # Test fetch with execution_id
            response_fetch_by_execution = cls.call_viewer("/viewer/query", {
                'database': cls.dedicated_db,
                'action': 'fetch-long-query',
                'execution_id': execution_id,
                'schema': 'multi'
            })
            result['fetch_by_execution_id'] = cls.normalize_result_query_long(response_fetch_by_execution)

            # Test error case - missing both operation_id and execution_id
            response_fetch_error = cls.call_viewer("/viewer/query", {
                'database': cls.dedicated_db,
                'action': 'fetch-long-query',
                'schema': 'multi'
            })
            result['fetch_error_no_id'] = response_fetch_error

            # Test error case - invalid operation_id
            response_fetch_invalid = cls.call_viewer("/viewer/query", {
                'database': cls.dedicated_db,
                'action': 'fetch-long-query',
                'operation_id': 'invalid-operation-id',
                'schema': 'multi'
            })
            result['fetch_invalid_operation_id'] = response_fetch_invalid

        return result

    @classmethod
    def call_viewer_multipart_parsed(cls, path, params=None):
        """Call viewer endpoint expecting multipart response and parse JSON parts"""
        if params is None:
            params = {}

        # Make raw HTTP request to get multipart response
        port = cls.cluster.nodes[1].mon_port
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

    @classmethod
    def normalize_multipart_response(cls, response):
        """Helper function to normalize multipart responses"""
        if 'multipart_parts' in response:
            normalized_parts = []
            for part in response['multipart_parts']:
                normalized_part = cls.normalize_result_query_long(part)
                normalized_parts.append(normalized_part)
            return {"multipart_parts": normalized_parts}
        return response

    @classmethod
    def test_viewer_query_long_multipart(cls):
        """Test execute-long-query with multipart streaming (schema=multipart)"""

        # Test successful long query with multipart streaming
        response_execute_stream = cls.call_viewer_multipart_parsed("/viewer/query", {
            'database': cls.dedicated_db,
            'action': 'execute-long-query',
            'query': 'SELECT * FROM table1 LIMIT 3;',
            'schema': 'multipart'
        })

        result = {}

        # Apply normalization to multipart responses
        result['execute_stream_response'] = cls.normalize_multipart_response(response_execute_stream)

        # Test error case with multipart streaming - invalid operation_id
        response_fetch_invalid_stream = cls.call_viewer_multipart_parsed("/viewer/query", {
            'database': cls.dedicated_db,
            'action': 'fetch-long-query',
            'operation_id': 'invalid-operation-id',
            'schema': 'multipart'
        })

        result['fetch_invalid_stream_response'] = cls.normalize_multipart_response(response_fetch_invalid_stream)

        # Test error case - missing operation_id/execution_id with multipart
        response_fetch_error_stream = cls.call_viewer_multipart_parsed("/viewer/query", {
            'database': cls.dedicated_db,
            'action': 'fetch-long-query',
            'schema': 'multipart'
        })

        result['fetch_error_stream_response'] = cls.normalize_multipart_response(response_fetch_error_stream)

        return result

    @classmethod
    def normalize_result_event_stream(cls, result):
        result = cls.replace_values_by_key(result, ['query_id',
                                                    'node_id',
                                                    'session_id',
                                                    ])
        return result

    @classmethod
    def test_viewer_query_event_stream(cls):
        """Execute a query requesting an event-stream (text/event-stream) response and parse it.

        The server should return SSE formatted data:
            event: <name> (optional, default 'message')\n
            data: <payload line 1>\n
            data: <payload line 2> (optional, concatenated with newline)\n
            \n (blank line terminates one event)

        We collect events, try to decode JSON payloads, and normalize dynamic fields.
        """
        body = {
            'database': cls.dedicated_db,
            'query': 'SELECT * FROM table1;',
            'action': 'execute-query',
            'schema': 'multipart',
        }
        response = cls.call_viewer_api_post("/viewer/query", body, headers={
            'Content-Type': 'application/json',
            'Accept': 'text/event-stream',
        })
        result = {
            'status_code': response.status_code,
            'content_type': response.headers.get('Content-Type')
        }

        # If not OK just return text for canonization
        if response.status_code != 200:
            result['text'] = response.text
            return result

        raw_text = response.text

        def parse_event_stream(text):
            events = []
            current_event = {}
            data_lines = []

            def flush_event():
                if not data_lines and 'event' not in current_event and 'id' not in current_event:
                    return
                data_str = '\n'.join(data_lines)
                data_value = data_str
                # Try JSON parse
                try:
                    parsed = json.loads(data_str)
                    if isinstance(parsed, dict):
                        parsed = cls.normalize_result_query_long(parsed)
                    data_value = parsed
                except Exception:
                    pass
                evt = {
                    'event': current_event.get('event', 'message'),
                    'data': cls.normalize_result_event_stream(data_value)
                }
                if 'id' in current_event:
                    evt['id'] = current_event['id']
                if 'retry' in current_event:
                    evt['retry'] = current_event['retry']
                events.append(evt)

            for line in text.splitlines():
                if line == '':  # end of event
                    flush_event()
                    current_event = {}
                    data_lines = []
                    continue
                if line.startswith(':'):
                    # comment line, ignore
                    continue
                if line.startswith('event:'):
                    current_event['event'] = line[6:].strip()
                elif line.startswith('data:'):
                    data_lines.append(line[5:].lstrip())
                elif line.startswith('id:'):
                    current_event['id'] = line[3:].strip()
                elif line.startswith('retry:'):
                    current_event['retry'] = line[6:].strip()
                else:
                    # treat as continuation of data
                    data_lines.append(line)
            # Flush last event if file didn't end with blank line
            flush_event()
            return events

        result['events'] = parse_event_stream(raw_text)
        return result

    @classmethod
    def test_security(cls):
        result = {}
        result['database_nodes_root'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'database': cls.dedicated_db,
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        })
        result['database_nodes_monitoring'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'database': cls.dedicated_db,
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        })
        result['database_nodes_viewer'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'database': cls.dedicated_db,
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        })
        result['database_nodes_database'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'database': cls.dedicated_db,
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        })

        result['cluster_nodes_root'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        })
        result['cluster_nodes_monitoring'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        })
        result['cluster_nodes_viewer'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        })
        result['cluster_nodes_database'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        })

        result['storage_nodes_root'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
            'database': cls.dedicated_db,
            'type': 'storage',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        })
        result['storage_nodes_monitoring'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
            'database': cls.dedicated_db,
            'type': 'storage',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        })
        result['storage_nodes_viewer'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
            'database': cls.dedicated_db,
            'type': 'storage',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        })
        result['storage_nodes_database'] = cls.get_viewer_normalized("/viewer/nodes", params={
            'fields_required': 'NodeId',
            'database': cls.dedicated_db,
            'type': 'storage',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        })

        result['down_node_root'] = cls.post_viewer("/tablets/app", params={
            'TabletID': '72057594037968897',
            'page': 'SetDown',
            'node': '1',
            'down': '0',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        })
        result['down_node_monitoring'] = cls.post_viewer("/tablets/app", params={
            'TabletID': '72057594037968897',
            'page': 'SetDown',
            'node': '1',
            'down': '0',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        })
        result['down_node_viewer'] = cls.post_viewer("/tablets/app", params={
            'TabletID': '72057594037968897',
            'page': 'SetDown',
            'node': '1',
            'down': '0',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        })
        result['down_node_database'] = cls.post_viewer("/tablets/app", params={
            'TabletID': '72057594037968897',
            'page': 'SetDown',
            'node': '1',
            'down': '0',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        })

        result['restart_pdisk_root'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        }), ['debugMessage'])
        result['restart_pdisk_monitoring'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        }), ['debugMessage'])
        result['restart_pdisk_viewer'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        }), ['debugMessage'])
        result['restart_pdisk_database'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        }), ['debugMessage'])

        result['restart_pdisk_database_force'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
            'force': '1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.database_session_id,
        }), ['debugMessage'])
        result['restart_pdisk_viewer_force'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
            'force': '1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.viewer_session_id,
        }), ['debugMessage'])
        result['restart_pdisk_monitoring_force'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
            'force': '1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.monitoring_session_id,
        }), ['debugMessage'])
        result['restart_pdisk_root_force'] = cls.replace_values_by_key(cls.post_viewer("/pdisk/restart", body={
            'pdisk_id': '1-1',
            'force': '1',
        }, headers={
            'Cookie': 'ydb_session_id=' + cls.root_session_id,
        }), ['debugMessage'])
        return result

    @classmethod
    def test_storage_stats(cls):
        result = {}
        tries = 15
        while tries > 0:
            result = cls.get_viewer_normalized("/viewer/storage_stats", {
                'database': cls.dedicated_db,
                'path': '.,table1,topic1',
            })
            if result['Paths'][2]['Groups'] != 0:
                break
            tries -= 1
            time.sleep(1)
        return result

    @classmethod
    def test_viewer_peers(cls):
        result = cls.get_viewer_normalized("/viewer/peers")
        cls.delete_keys_recursively(result, {'ScopeId', 'PoolStats'})
        return result
