# -*- coding: utf-8 -*-
import errno
import json
import os
import socket
import ssl
import time

import pytest
import requests

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.tls_tools import generate_selfsigned_cert
from ydb.tests.oss.ydb_sdk_import import ydb


# Syntactically framed but undecodable: ydbd accepts it as a non-empty monitoring
# certificate, while the HTTP acceptor cannot build a security context out of it.
MALFORMED_PEM = '-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n'

SECURITY_CONTEXT_MARKER = 'Failed to construct server security context'
RETRY_MARKER = 'Failed to init - retrying...'


def _self_signed_pem(tmp_dir):
    """Returns an inline cert+key PEM for the monitoring endpoint and a CA file to verify it."""
    cert_pem, key_pem = generate_selfsigned_cert('localhost')
    ca_path = os.path.join(str(tmp_dir), 'mon_ca.pem')
    with open(ca_path, 'wb') as ca_file:
        ca_file.write(cert_pem)
    return (cert_pem + key_pem).decode('ascii'), ca_path


def _configurator(inline_pem=None):
    configurator = KikimrConfigGenerator()
    if inline_pem is not None:
        configurator.yaml_config.setdefault('monitoring_config', {})['monitoring_certificate'] = inline_pem
    return configurator


def _count_marker(log_path, marker):
    if not log_path or not os.path.exists(log_path):
        return 0
    with open(log_path, 'rb') as log_file:
        return log_file.read().decode('utf-8', errors='replace').count(marker)


def _wait_for_marker(log_path, marker, count, timeout):
    deadline = time.time() + timeout
    seen = 0
    while time.time() < deadline:
        seen = _count_marker(log_path, marker)
        if seen >= count:
            return seen
        time.sleep(0.5)
    raise AssertionError(
        'expected at least %d occurrences of %r in %s within %ds, got %d' % (count, marker, log_path, timeout, seen)
    )


def _probe_mon_port(port, tls_timeout=3.0):
    """Describes what a client sees on the monitoring port: refused, or connected plus TLS outcome."""
    try:
        connection = socket.create_connection(('127.0.0.1', port), timeout=3.0)
    except OSError as e:
        return {'tcp_connect': 'failed', 'errno': e.errno, 'errno_name': errno.errorcode.get(e.errno, str(e.errno))}
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        connection.settimeout(tls_timeout)
        with context.wrap_socket(connection, server_hostname='localhost'):
            return {'tcp_connect': 'connected', 'tls_handshake': 'ok'}
    except (socket.timeout, TimeoutError):
        return {'tcp_connect': 'connected', 'tls_handshake': 'timeout'}
    except ssl.SSLError as e:
        return {'tcp_connect': 'connected', 'tls_handshake': 'ssl_error: %s' % type(e).__name__}
    except OSError as e:
        return {'tcp_connect': 'connected', 'tls_handshake': 'error: %s' % errno.errorcode.get(e.errno, str(e.errno))}
    finally:
        try:
            connection.close()
        except OSError:
            pass


def _assert_mon_port_refused(port, label):
    probe = _probe_mon_port(port)
    # Printed unconditionally: on a broken build this line is the recorded symptom.
    print('MON_PORT_PROBE %s %s' % (label, json.dumps(probe, sort_keys=True)))
    assert probe.get('errno') == errno.ECONNREFUSED, (
        'monitoring port %d accepts TCP connections without a usable TLS context: %s' % (port, probe)
    )


def _sql_smoke(node, table_path):
    driver_config = ydb.DriverConfig(
        endpoint='%s:%s' % (node.host, node.grpc_port),
        database='/Root',
        credentials=ydb.AnonymousCredentials(),
    )
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=60, fail_fast=True)
        with ydb.QuerySessionPool(driver) as pool:
            result_sets = pool.execute_with_retries('SELECT 1 AS value;')
            assert result_sets[0].rows[0].value == 1

            pool.execute_with_retries(
                'CREATE TABLE `%s` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key));' % table_path
            )
            pool.execute_with_retries('UPSERT INTO `%s` (key, value) VALUES (1, "ready");' % table_path)
            result_sets = pool.execute_with_retries('SELECT value FROM `%s` WHERE key = 1;' % table_path)
            stored = result_sets[0].rows[0].value
            if isinstance(stored, bytes):
                stored = stored.decode('utf-8')
            assert stored == 'ready'


def test_invalid_inline_pem_does_not_listen():
    """An unusable monitoring certificate must leave no listening port behind.

    The node itself keeps running: ydbd does not wait for the monitoring listener.
    """
    cluster = KiKiMR(_configurator(MALFORMED_PEM))
    # Only the node is started: cluster initialization polls monitoring, which is broken here.
    cluster.prepare()
    node = cluster.nodes[1]
    try:
        cluster.start_node(1)
        log_path = node.ydbd_log_file_path
        assert log_path, 'use_log_files must provide the ydbd log path'

        _wait_for_marker(log_path, SECURITY_CONTEXT_MARKER, 1, 60)
        for attempt in range(1, 4):
            _wait_for_marker(log_path, RETRY_MARKER, attempt, 30)
            _assert_mon_port_refused(node.mon_port, 'retry_%d' % attempt)

        assert node.is_alive(), 'ydbd must keep running when the monitoring listener cannot start'
    finally:
        cluster.stop()


@pytest.mark.parametrize('scheme', ['http', 'https'])
def test_valid_listener_serves_queries(scheme, tmp_path):
    ca_path = None
    inline_pem = None
    if scheme == 'https':
        inline_pem, ca_path = _self_signed_pem(tmp_path)

    cluster = KiKiMR(_configurator(inline_pem))
    cluster.prepare()
    node = cluster.nodes[1]
    node.mon_uses_https = scheme == 'https'
    try:
        assert cluster.start() is not None, 'cluster with a %s monitoring endpoint failed to start' % scheme

        response = requests.get(
            '%s://%s:%d/ping' % (scheme, node.host, node.mon_port),
            verify=ca_path if ca_path else True,
            timeout=10,
        )
        assert response.status_code == 200

        _sql_smoke(node, '/Root/mon_tls_startup_%s' % scheme)
    finally:
        cluster.stop()


def test_restart_with_repaired_inline_pem(tmp_path):
    """After the certificate is replaced, a restart must bring the listener and the node back."""
    good_pem, ca_path = _self_signed_pem(tmp_path)

    configurator = _configurator(MALFORMED_PEM)
    cluster = KiKiMR(configurator)
    cluster.prepare()
    node = cluster.nodes[1]
    try:
        cluster.start_node(1)
        broken_log_path = node.ydbd_log_file_path
        assert broken_log_path, 'use_log_files must provide the ydbd log path'
        _wait_for_marker(broken_log_path, SECURITY_CONTEXT_MARKER, 1, 60)
        _wait_for_marker(broken_log_path, RETRY_MARKER, 2, 30)
        _assert_mon_port_refused(node.mon_port, 'before_repair')

        node.stop()

        configurator.yaml_config['monitoring_config']['monitoring_certificate'] = good_pem
        configurator.write_proto_configs(cluster.config_path)
        # Keep the log of the failed start instead of truncating it on restart.
        node.set_log_file_prefix('logfile_repaired_')
        node.mon_uses_https = True

        assert cluster.start() is not None, 'cluster failed to start with the repaired certificate'

        response = requests.get(
            'https://%s:%d/ping' % (node.host, node.mon_port),
            verify=ca_path,
            timeout=10,
        )
        assert response.status_code == 200

        repaired_log_path = node.ydbd_log_file_path
        assert _count_marker(repaired_log_path, SECURITY_CONTEXT_MARKER) == 0, (
            'repaired node still failed to construct the monitoring security context'
        )

        _sql_smoke(node, '/Root/mon_tls_startup_restart')
    finally:
        cluster.stop()
