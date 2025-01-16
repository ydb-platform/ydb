#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import itertools
import logging
import time
import requests
import json
import uuid
import re
import socket
from hamcrest import assert_that, equal_to, not_none, none, greater_than, less_than_or_equal_to, any_of, not_

import yatest

import ydb.tests.library.common.yatest_common as yatest_common

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

from ydb.tests.library.sqs.tables import create_all_tables as create_all_sqs_tables
from ydb.tests.library.sqs.requests_client import SqsHttpApi
from ydb.tests.library.sqs.matchers import ReadResponseMatcher

from ydb.tests.oss.ydb_sdk_import import ydb
from concurrent import futures


DEFAULT_VISIBILITY_TIMEOUT = 30
DEFAULT_TABLES_FORMAT = 1


logger = logging.getLogger(__name__)


IS_FIFO_PARAMS = {
    'argnames': 'is_fifo',
    'argvalues': [True, False],
    'ids': ['fifo', 'std'],
}

TABLES_FORMAT_PARAMS = {
    'argnames': 'tables_format',
    'argvalues': [0, 1],
    'ids': ['tables_format_v0', 'tables_format_v1'],
}

POLLING_PARAMS = {
    'argnames': 'polling_wait_timeout',
    'argvalues': [1],
    'ids': ['long_polling'],
}

STOP_NODE_PARAMS = {
    'argnames': 'stop_node',
    'argvalues': [True, False],
    'ids': ['stop_node', 'kick_tablets'],
}

VISIBILITY_CHANGE_METHOD_PARAMS = {
    'argnames': 'delete_message',
    'argvalues': [True, False],
    'ids': ['with_delete_message', 'with_change_visibility'],
}

HAS_QUEUES_PARAMS = {
    'argnames': 'has_queues',
    'argvalues': [True, False],
    'ids': ['with_queues', 'without_queues']
}


def get_sqs_client_path():
    if os.getenv("SQS_CLIENT_BINARY"):
        return yatest_common.binary_path(os.getenv("SQS_CLIENT_BINARY"))
    raise RuntimeError("SQS_CLIENT_BINARY enviroment variable is not specified")


def to_bytes(v):
    if v is None:
        return v

    if isinstance(v, bytes):
        return v

    return v.encode('utf-8')


def wait_can_list_users(api):
    retries = 120
    while retries > 0:
        retries -= 1

        logger.info("Listing SQS users")

        try:
            api.list_queues()
            logger.info("Success. Api is up.")
            return True

        except (requests.ConnectionError, requests.exceptions.Timeout):
            time.sleep(1)
            continue

        except Exception:
            logger.info("Success. Api is up.")
            return True

    return False


def get_fqdn():
    # the same implementation as
    # https://a.yandex-team.ru/arc/trunk/arcadia/util/system/hostname.cpp?rev=3541264#L36
    # that is used in ydb.
    hostname = socket.gethostname()
    addrinfo = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, 0, 0, socket.AI_CANONNAME)
    for ai in addrinfo:
        canonname_candidate = ai[3]
        if canonname_candidate:
            return canonname_candidate

    assert False, 'Failed to get FQDN'


def get_test_with_sqs_tenant_installation(base_test_class):
    class TestWithTenant(base_test_class):
        slot_count = 1
        database = '/Root/TenantSQS'
        sqs_root = '/Root/TenantSQS'

        @classmethod
        def _init_cluster(cls, cluster, config_generator):
            cluster.create_database(
                cls.database,
                storage_pool_units_count={
                    'hdd': 1
                }
            )
            cluster.register_and_start_slots(cls.database, count=1)
            cluster.wait_tenant_up(cls.database)
            super(TestWithTenant, cls)._init_cluster(cluster, config_generator)

    return TestWithTenant


def get_test_with_sqs_installation_by_path(base_test_class):
    class TestWithPath(base_test_class):
        database = '/Root'
        sqs_root = '/Root/PathSQS'

    return TestWithPath


class KikimrSqsTestBase(object):
    erasure = None
    slot_count = 0
    database = '/Root'
    sqs_root = '/Root/SQS'
    use_in_memory_pdisks = True
    tables_format_per_user = {}

    @classmethod
    def setup_class(cls):
        cls.cluster, cls.config_generator = cls._setup_cluster()
        cls.sqs_ports = []
        if cls.slot_count:
            cls.cluster_nodes = list(cls.cluster.slots.values())
        else:
            cls.cluster_nodes = list(cls.cluster.nodes.values())
        cls.cluster_nodes_count = len(cls.cluster_nodes)
        for node in cls.cluster_nodes:
            cls.sqs_ports.append(node.sqs_port)

        cls.sqs_port = cls.sqs_ports[0]
        cls.server_fqdn = get_fqdn()

    def _before_test_start(self):
        pass

    def setup_method(self, method=None):
        logging.debug('Test started: {}'.format(str(method.__name__)))
        logging.debug("Kikimr logs dir: {}".format(self.cluster.slots[1].cwd if self.slot_count else self.cluster.nodes[1].cwd))

        # Start all nodes in case of previous test with killed nodes
        for node_index in range(len(self.cluster.nodes)):
            self.cluster.nodes[node_index + 1].start()  # start if not alive

        for slot_index in range(len(self.cluster.slots)):
            self.cluster.slots[slot_index + 1].start()  # start if not alive

        for slot_index in range(len(self.cluster.slots)):
            self._enable_tablets_on_node(slot_index)

        grpc_port = self.cluster.slots[1].grpc_port if self.slot_count else self.cluster.nodes[1].grpc_port
        self._sqs_server_opts = ['-s', 'localhost', '-p', str(grpc_port)]
        test_name = str(method.__name__)[5:]

        def create_unique_name(user=False):
            max_length = 80 - (0 if user else len('.fifo'))
            name = '{subject}_{test}_{uid}'.format(
                subject=('U' if user else 'Q'),
                test=test_name[:60],
                uid=uuid.uuid1()
            )
            return name[:max_length]

        self._username = create_unique_name(user=True)
        self.queue_name = create_unique_name()
        self._msg_body_template = self._username + '-{}'
        self._setup_user(self._username)
        self._sqs_apis = []
        for node in self.cluster_nodes:
            self._sqs_apis.append(
                SqsHttpApi(
                    'localhost',
                    node.sqs_port,
                    self._username,
                    raise_on_error=True,
                    timeout=None
                )
            )

        tp = futures.ThreadPoolExecutor(8)
        fs = []
        for api in self._sqs_apis:
            fs.append(
                tp.submit(
                    wait_can_list_users, api
                )
            )

        for f in fs:
            f.result()

        self._sqs_api = self._sqs_apis[0]

        self._driver = self._make_kikimr_driver()

        self.counter = itertools.count()
        self.message_ids = []
        self.read_result = []

        self.seq_no = 0

        self._before_test_start()

    def teardown_method(self, method=None):
        self.check_no_queues_table(self._username)
        self._driver.stop()

        logging.debug(
            'Test finished: {}'.format(
                str(
                    method.__name__
                )
            )
        )

    def get_tables_format(self, user=None):
        if user is None:
            user = self._username
        return self.tables_format_per_user.get(user, DEFAULT_TABLES_FORMAT)

    def check_all_users_queues_tables_consistency(self):
        users = [entry.name for entry in self._driver.scheme_client.list_directory(self.sqs_root).children]
        for user in users:
            if user == '.AtomicCounter' or user == '.Settings' or user == '.Queues':
                continue
            self.check_no_queues_table(user)

    def check_no_queues_table(self, username):
        raised = False
        try:
            self._driver.scheme_client.describe_path('{}/{}/Queues'.format(self.sqs_root, username))
        except Exception as e:
            logger.debug(f'No queue table: {e}')
            raised = True  # Expect SchemeError or at least ConnectionLost in tests with node killings

        assert_that(raised)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @classmethod
    def _setup_config_generator(cls):
        config_generator = KikimrConfigGenerator(
            erasure=cls.erasure,
            use_in_memory_pdisks=cls.use_in_memory_pdisks,
            additional_log_configs={'SQS': LogLevels.DEBUG},
            enable_sqs=True,
        )
        config_generator.yaml_config['sqs_config']['root'] = cls.sqs_root
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = True
        config_generator.yaml_config['sqs_config']['masters_describer_update_time_ms'] = 2000
        config_generator.yaml_config['sqs_config']['max_number_of_receive_messages'] = 100
        config_generator.yaml_config['sqs_config']['transaction_timeout_ms'] = 60000
        config_generator.yaml_config['sqs_config']['master_connect_timeout_ms'] = 60000
        config_generator.yaml_config['sqs_config']['check_all_shards_in_receive_message'] = True
        config_generator.yaml_config['sqs_config']['create_legacy_duration_counters'] = False
        config_generator.yaml_config['sqs_config']['validate_message_body'] = True

        return config_generator

    @classmethod
    def _init_cluster(cls, cluster, config_generator):
        driver_config = ydb.DriverConfig(
            "%s:%s" % (cluster.nodes[1].host, cluster.nodes[1].port),
            cls.database
        )

        with ydb.Driver(driver_config) as driver:
            driver.wait()
            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    create_all_sqs_tables(cls.sqs_root, driver, session)
        cls.create_metauser(cluster, config_generator)

    @classmethod
    def create_metauser(cls, cluster, config_generator):
        grpc_port = cluster.slots[1].grpc_port if cls.slot_count else cluster.nodes[1].grpc_port
        cmd = [
            get_sqs_client_path(),
            'user',
            '-u', 'metauser',
            '-n', 'metauser',
            '-s', 'localhost',
            '-p', str(grpc_port)
        ]
        yatest_common.execute(cmd)

    @classmethod
    def _setup_cluster(cls):
        config_generator = cls._setup_config_generator()
        cluster = kikimr_cluster_factory(config_generator)
        cluster.start()
        cls._init_cluster(cluster, config_generator)
        return cluster, config_generator

    def _setup_user(self, _username, retries_count=20):
        cmd = [
            get_sqs_client_path(),
            'user',
            '-u', 'metauser',
            '-n', _username,
        ] + self._sqs_server_opts
        while retries_count:
            logging.debug("Running {}".format(' '.join(cmd)))
            try:
                yatest_common.execute(cmd)
            except yatest_common.ExecutionError as ex:
                logging.debug("Create user failed: {}. Retrying".format(ex))
                retries_count -= 1
                time.sleep(3)
            else:
                return
        raise RuntimeError("Failed to create SQS user")

    def _create_api_for_user(self, user_name, raise_on_error=True, security_token=None, force_private=False, iam_token=None, folder_id=None):
        api = SqsHttpApi(self.cluster.nodes[1].host,
                         self.cluster_nodes[0].sqs_port,
                         user_name,
                         raise_on_error=raise_on_error,
                         timeout=None,
                         security_token=security_token,
                         force_private=force_private,
                         iam_token=iam_token,
                         folder_id=folder_id
                         )
        return api

    def _make_kikimr_driver(self, node_index=0):
        if self.slot_count:
            port = self.cluster.slots[node_index + 1].port
        else:
            port = self.cluster.nodes[node_index + 1].port
        connection_params = ydb.ConnectionParams("localhost:{}".format(port))
        connection_params.set_database(self.database)
        driver = ydb.Driver(connection_params)
        driver.wait()
        return driver

    def _execute_yql_query(self, query_text):
        logging.debug('Executing database query: "{}"'.format(query_text))
        retries = 5
        retries_left = retries
        while retries_left:
            retries_left -= 1
            try:
                session = self._driver.table_client.session().create()
                data_result_sets = session.transaction().execute(query_text, commit_tx=True)
                return data_result_sets
            except (ydb.ConnectionError, ydb.Timeout, ydb.BadSession, ydb.Unavailable, ydb.InternalError) as ex:
                logging.warning('Kikimr driver exception: {}'.format(ex))
                # SQS-307
                if retries_left:
                    logging.info('Retrying query')
                    time.sleep(1)
                else:
                    raise

    def _queue_url_matcher(self, queue_name):
        urls_matchers = [
            equal_to(
                to_bytes(
                    str('http://{server}:{port}/{user}/{queue_name}').format(
                        server=self.server_fqdn, port=port, user=self._username, queue_name=queue_name
                    )
                )
            )
            for port in self.sqs_ports
        ]
        return any_of(*urls_matchers)

    def _create_queue_and_assert(self, queue_name, is_fifo=False, use_http=False, attributes=None, shards=None, retries=3, tags=None):
        self.queue_url = None
        if attributes is None:
            attributes = {}
        if tags is None:
            tags = {}
        logging.debug('Create queue. Name: {}. Attributes: {}. Use http: {}. Is fifo: {}'.format(queue_name, attributes, use_http, is_fifo))

        assert (len(attributes.keys()) == 0 or use_http), 'Attributes are supported only for http queue creation'
        assert (len(tags.keys()) == 0 or use_http), 'Tags are supported only for http queue creation'
        assert (shards is None or not use_http), 'Custom shards number is only supported in non-http mode'
        while retries:
            retries -= 1
            try:
                self.queue_url = self._sqs_api.create_queue(queue_name, is_fifo=is_fifo, attributes=attributes, tags=tags)
            except (RuntimeError, yatest.common.ExecutionError) as ex:
                logging.debug("Got error: {}. Retrying creation request".format(ex))
                if retries:
                    time.sleep(1)  # Sleep before next retry
                else:
                    raise
            if self.queue_url is not None:  # queue_url will be None in case of connection error
                break
        assert_that(
            to_bytes(self.queue_url),
            self._queue_url_matcher(queue_name)
        )
        return self.queue_url

    def _send_message_and_assert(self, queue_url, msg_body, seq_no=None, group_id=None, attributes=None, delay_seconds=None):
        attributes = {} if attributes is None else attributes
        send_msg_result = self._sqs_api.send_message(
            queue_url, msg_body, deduplication_id=seq_no, group_id=group_id, attributes=attributes, delay_seconds=delay_seconds
        )
        assert_that(
            send_msg_result, not_none()
        )
        return send_msg_result

    def _send_messages(self, queue_url, message_count, msg_body_template=None, is_fifo=False, group_id=None):
        if msg_body_template is None:
            msg_body_template = self._msg_body_template
        ret = []
        for _ in range(message_count):
            if is_fifo:
                result = self._send_message_and_assert(
                    queue_url, msg_body_template.format(next(self.counter)), seq_no=self.seq_no, group_id=group_id
                )
                self.seq_no += 1
            else:
                result = self._send_message_and_assert(queue_url, msg_body_template.format(next(self.counter)))
            ret.append(result)
        return ret

    def _read_while_not_empty(self, queue_url, messages_count, visibility_timeout=None, wait_timeout=1, max_empty_reads=1):
        ret = []
        messages_by_time = {}
        actual_vis_timeout = visibility_timeout if visibility_timeout is not None else DEFAULT_VISIBILITY_TIMEOUT
        empty_reads_count = 0
        max_batch_to_read = self.config_generator.yaml_config['sqs_config']['max_number_of_receive_messages']
        while len(ret) < messages_count:
            # noinspection PyTypeChecker
            request_start = time.time()
            read_result = self._sqs_api.receive_message(
                queue_url, max_number_of_messages=min(messages_count - len(ret), max_batch_to_read),
                visibility_timeout=visibility_timeout, wait_timeout=wait_timeout
            )
            if not read_result:
                empty_reads_count += 1
                if empty_reads_count == max_empty_reads:
                    break
            else:
                empty_reads_count = 0

            if read_result:
                request_end = time.time()
                for msg in read_result:
                    msg_id = msg['MessageId']
                    if msg_id not in messages_by_time:
                        messages_by_time[msg_id] = request_start
                        ret.append(msg)
                    elif request_end > messages_by_time[msg_id] + actual_vis_timeout:
                        messages_by_time[msg_id] = max(
                            request_start, messages_by_time[msg_id] + actual_vis_timeout
                        )
                    else:
                        raise AssertionError("Message {} appeared twice before visibility timeout expired".format(msg_id))
        return ret

    def _read_messages_and_assert(
            self, queue_url, messages_count, matcher=None, visibility_timeout=None, wait_timeout=1
    ):
        read_result = self._read_while_not_empty(
            queue_url, messages_count=messages_count,
            visibility_timeout=visibility_timeout, wait_timeout=wait_timeout
        )
        if matcher is not None:
            assert_that(
                read_result, matcher
            )
        return read_result

    def _create_queue_send_x_messages_read_y_messages(
            self, queue_name, send_count, read_count, msg_body_template,
            is_fifo=False, visibility_timeout=None, wait_timeout=1, group_id="1"
    ):
        self._create_queue_and_assert(queue_name, is_fifo)
        if is_fifo:
            self.message_ids = self._send_messages(
                self.queue_url, send_count, msg_body_template, is_fifo=True, group_id=group_id
            )
        else:
            self.message_ids = self._send_messages(
                self.queue_url, send_count, msg_body_template
            )
        self.read_result = self._read_messages_and_assert(
            self.queue_url, read_count,
            ReadResponseMatcher().with_some_of_message_ids(self.message_ids).with_n_messages(read_count),
            visibility_timeout, wait_timeout
        )

    def _other_node(self, node_index):
        if node_index == 0:
            return 1
        else:
            return node_index - 1

    def _get_live_node_index(self):
        for i in range(self.cluster_nodes_count):
            if self.slot_count:
                if self.cluster.slots[i + 1].is_alive():
                    return i
            else:
                if self.cluster.nodes[i + 1].is_alive():
                    return i
        assert False, 'alive node not found'

    def _get_mon_port(self, node_index):
        if self.slot_count:
            return self.config_generator.port_allocator.get_slot_port_allocator(node_index + 1).mon_port
        else:
            return self.config_generator.port_allocator.get_node_port_allocator(node_index + 1).mon_port

    def _get_sqs_counters(self, node_index=0, counters_format='json'):
        return self._get_counters(node_index, "sqs", counters_format)

    def _get_ymq_counters(self, cloud, folder, node_index=0, counters_format='json'):
        return self._get_counters(node_index, "ymq_public", counters_format, cloud=cloud, folder=folder)

    def _get_counters(self, node_index, component, counters_format, cloud=None, folder=None, dump_to_log=True):
        mon_port = self._get_mon_port(node_index)

        if counters_format == 'json':
            format_suffix = '/json'
        elif counters_format == 'text':
            format_suffix = ''
        else:
            raise Exception('Unknown counters format: \"{}\"'.format(counters_format))

        if folder is not None:
            labels = "/cloud%3D{cloud}/folder%3D{folder}".format(cloud=cloud, folder=folder)
        else:
            labels = ''
        counters_url = 'http://localhost:{port}/counters/counters%3D{component}{labels}{suffix}'.format(
            port=mon_port, component=component, suffix=format_suffix, labels=labels
        )
        reply = requests.get(counters_url)
        assert_that(reply.status_code, equal_to(200))

        if counters_format == 'json':
            ret = reply.json()
        else:
            ret = reply.text

        assert_that(ret, not_none())
        if dump_to_log:
            logging.debug('Got counters from node {}:\n{}'.format(node_index, json.dumps(ret, sort_keys=True, indent=2)))
        return ret

    def _get_counter(self, counters, labels):
        logging.debug('Searching for counter with labels:\n{}'.format(json.dumps(labels, sort_keys=True, indent=2)))
        for sensor in counters['sensors']:
            sensor_labels = sensor['labels']
            found = True
            for label in labels:
                if labels[label] != sensor_labels.get(label):
                    found = False
                    break
            if found:
                logging.debug('Return counter:\n{}'.format(json.dumps(sensor, sort_keys=True, indent=2)))
                return sensor
        logging.debug('No counter with labels found:\n{}'.format(json.dumps(labels, sort_keys=True, indent=2)))

    def _get_counter_value(self, counters, labels, default_value=None):
        sensor = self._get_counter(counters, labels)
        return sensor['value'] if sensor is not None else default_value

    def _kick_tablets_from_node(self, node_index):
        mon_port = self._get_mon_port(0)

        toggle_down_url = 'http://localhost:{}/tablets/app?TabletID=72057594037968897&node={}&page=SetDown&down=1'.format(mon_port, node_index + 1)
        toggle_down_reply = requests.get(toggle_down_url)
        assert_that(toggle_down_reply.status_code, equal_to(200))
        logging.debug('Toggle down reply: {}'.format(toggle_down_reply.text))

        kick_url = 'http://localhost:{}/tablets/app?TabletID=72057594037968897&node={}&page=KickNode'.format(mon_port, node_index + 1)
        kick_reply = requests.get(kick_url)
        assert_that(kick_reply.status_code, equal_to(200))
        logging.debug('Kick reply: {}'.format(kick_reply.text))

        time.sleep(3)

    def _enable_tablets_on_node(self, node_index):
        # make several attempts because this function is needed to be called right after node's start
        attempts = 30
        while attempts:
            attempts -= 1

            try:
                mon_port = self._get_mon_port(0)
                toggle_up_url = 'http://localhost:{}/tablets/app?TabletID=72057594037968897&node={}&page=SetDown&down=0'.format(mon_port, node_index + 1)
                toggle_up_reply = requests.get(toggle_up_url)

                logging.debug('Toggle up reply: {}'.format(toggle_up_reply.text))
                if toggle_up_reply.status_code != 200 and attempts:
                    logging.debug('Non 200 error code: {}. Retrying'.format(toggle_up_reply.status_code))
                    time.sleep(1)
                    continue

                assert_that(toggle_up_reply.status_code, equal_to(200))
                break
            except requests.ConnectionError:
                if attempts == 0:
                    raise
                logging.debug('Connection error: trying to enable tablets on node {} in 1 second'.format(node_index))
                time.sleep(1)  # wait node to start

    def _smart_make_table_path(self, user_name, queue_name, queue_version, shard, table_name):
        tables_format = self.get_tables_format(user_name)

        table_path = self.sqs_root
        if tables_format == 0:
            table_path += '/{}'.format(user_name)
            if queue_name is not None:
                table_path += '/{}'.format(queue_name)
            if queue_version is not None and queue_version != 0:
                table_path += '/v{}'.format(queue_version)
            if shard is not None:
                table_path += '/{}'.format(shard)
        else:
            table_path += '/{}'.format('.FIFO' if queue_name.endswith('.fifo') else '.STD')
        return table_path + '/{}'.format(table_name)

    def _get_queue_version_number(self, user_name, queue_name):
        table_path = '{}/.Queues'.format(self.sqs_root)
        data_result_sets = self._execute_yql_query('SELECT Version FROM `{}` WHERE Account=\'{}\' AND QueueName=\'{}\''.format(table_path, user_name, queue_name))
        assert_that(len(data_result_sets), equal_to(1))
        assert_that(len(data_result_sets[0].rows), equal_to(1))

        version = data_result_sets[0].rows[0]['Version']
        if version is None:
            return 0
        else:
            return int(version)

    def _get_queue_master_tablet_id(self, user_name_param=None, queue_name_param=None):
        user_name = user_name_param if user_name_param else self._username
        queue_name = queue_name_param if queue_name_param else self.queue_name
        table_path = '{}/.Queues'.format(self.sqs_root)
        data_result_sets = self._execute_yql_query('SELECT MasterTabletId FROM `{}` WHERE Account=\'{}\' AND QueueName=\'{}\''.format(table_path, user_name, queue_name))
        assert_that(len(data_result_sets), equal_to(1))
        assert_that(len(data_result_sets[0].rows), equal_to(1))

        tablet_id = data_result_sets[0].rows[0]['MasterTabletId']
        assert_that(tablet_id, not_(none()))
        return int(tablet_id)

    def _get_queue_master_node_index(self, user_name=None, queue_name=None):
        tablet_id = self._get_queue_master_tablet_id(user_name, queue_name)
        mon_port = self._get_mon_port(self._get_live_node_index())

        tablet_info_url = 'http://localhost:{}/tablets?TabletID={}'.format(mon_port, tablet_id)

        retries = 5
        retries_left = retries
        while retries_left:
            retries_left -= 1
            tablet_info_reply = requests.get(tablet_info_url)
            if tablet_info_reply.status_code != 200:
                time.sleep(1)
                continue
            logging.debug('Tablet info reply: {}'.format(tablet_info_reply.text))

            node_id_match = re.search('NodeID:\\s(\\d+)', tablet_info_reply.text)
            if node_id_match is None:
                time.sleep(1)
                continue
            node_index = int(node_id_match.group(1))
            assert_that(node_index, greater_than(0))
            assert_that(node_index, less_than_or_equal_to(self.cluster_nodes_count))
            return node_index - 1
        assert False, "Couldn't get tablet info from viewer in {} tries".format(retries)

    def _get_queue_shards_count(self, username, queuename, queue_version):
        state_table_path = self._smart_make_table_path(username, queuename, queue_version, None, 'State')
        return self._get_table_lines_count(state_table_path)

    def _check_queue_tables_are_empty(self, queue_name=None):
        if queue_name is None:
            queue_name = self.queue_name
        is_fifo = queue_name.endswith('.fifo')
        queue_version = self._get_queue_version_number(self._username, queue_name)
        tables_format = self.tables_format_per_user.get(self._username, 0)

        self.__check_queue_tables_are_empty(queue_name, is_fifo, queue_version, tables_format)

    def _get_table_lines_count(self, table_path, condition=None):
        query = 'SELECT COUNT(*) AS count FROM `{}` {};'.format(table_path, condition if condition else '')
        data_result_sets = self._execute_yql_query(query)
        assert_that(len(data_result_sets), equal_to(1))
        assert_that(len(data_result_sets[0].rows), equal_to(1))
        logging.debug('Received count result for table {}: {}'.format(table_path, data_result_sets[0].rows[0]))
        return data_result_sets[0].rows[0]['count']

    def __check_queue_tables_are_empty(self, queue_name, is_fifo, queue_version, tables_format):
        shards = [None] if is_fifo else range(self._get_queue_shards_count(self._username, queue_name, queue_version))
        rows_condition = f' WHERE QueueIdNumber = {queue_version}' if tables_format == 1 else None

        table_names = ['Messages', 'SentTimestampIdx']
        table_names += ['Data', 'Groups'] if is_fifo else ['Infly', 'MessageData']
        for shard in shards:
            for table_name in table_names:
                table_path = self._smart_make_table_path(self._username, queue_name, queue_version, shard, table_name)
                rows_count = self._get_table_lines_count(table_path, rows_condition)
                assert_that(rows_count, equal_to(0))

    def _break_queue(self, username, queuename, is_fifo):
        version = self._get_queue_version_number(username, queuename)
        session = self._driver.table_client.session().create()
        if is_fifo:
            session.drop_table(self._smart_make_table_path(username, queuename, version, None, 'Messages'))
            session.drop_table(self._smart_make_table_path(username, queuename, version, None, 'Data'))
        else:
            shards = self._get_queue_shards_count(username, queuename, version)
            for shard in range(shards):
                session.drop_table(self._smart_make_table_path(username, queuename, version, shard, 'Messages'))
                session.drop_table(self._smart_make_table_path(username, queuename, version, shard, 'MessageData'))

    def _set_tables_format(self, username=None, tables_format=1):
        if username is None:
            username = self._username
        self._execute_yql_query(
            'UPSERT INTO `{}/.Settings` (Account, Name, Value) \
                VALUES ("{}", "CreateQueuesWithTabletFormat", "{}")'.format(
                self.sqs_root,
                username,
                tables_format
            )
        )
        self.tables_format_per_user[username] = tables_format

    def _init_with_params(self, is_fifo=None, tables_format=None):
        if is_fifo and not self.queue_name.endswith('.fifo'):
            self.queue_name += '.fifo'
        if tables_format is not None:
            self._set_tables_format(tables_format=tables_format)
