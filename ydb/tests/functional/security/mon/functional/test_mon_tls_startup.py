# -*- coding: utf-8 -*-
import errno
import json
import os
import socket
import ssl
import time

import requests

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.tls_tools import generate_selfsigned_cert


# Syntactically framed but undecodable: ydbd accepts it as a non-empty monitoring
# certificate, while the HTTP acceptor cannot build a security context out of it.
MALFORMED_PEM = '-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n'

SECURITY_CONTEXT_MARKER = 'Failed to construct server security context'
RETRY_MARKER = 'Failed to init - retrying...'


def _configurator(inline_pem):
    configurator = KikimrConfigGenerator()
    configurator.yaml_config.setdefault('monitoring_config', {})['monitoring_certificate'] = inline_pem
    return configurator


def _count_marker(log_path, marker):
    with open(log_path, 'rb') as log_file:
        return log_file.read().decode('utf-8', errors='replace').count(marker)


def _wait_for_marker(log_path, marker, count, timeout):
    deadline = time.time() + timeout
    seen = 0
    while time.time() < deadline:
        seen = _count_marker(log_path, marker)
        if seen >= count:
            return
        time.sleep(0.5)
    raise AssertionError(
        'expected at least %d occurrences of %r in %s within %ds, got %d' % (count, marker, log_path, timeout, seen)
    )


def _probe_mon_port(port):
    """Describes what a client sees on the monitoring port: refused, or connected plus TLS outcome."""
    try:
        connection = socket.create_connection(('127.0.0.1', port), timeout=3.0)
    except OSError as e:
        return {'tcp_connect': 'failed', 'errno': e.errno, 'errno_name': errno.errorcode.get(e.errno, str(e.errno))}
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        connection.settimeout(3.0)
        with context.wrap_socket(connection, server_hostname='localhost'):
            return {'tcp_connect': 'connected', 'tls_handshake': 'ok'}
    except (socket.timeout, TimeoutError):
        return {'tcp_connect': 'connected', 'tls_handshake': 'timeout'}
    except (ssl.SSLError, OSError) as e:
        return {'tcp_connect': 'connected', 'tls_handshake': 'error: %s' % type(e).__name__}
    finally:
        connection.close()


def _assert_mon_port_refused(port, label):
    probe = _probe_mon_port(port)
    # Printed unconditionally: on a broken build this line is the recorded symptom.
    print('MON_PORT_PROBE %s %s' % (label, json.dumps(probe, sort_keys=True)))
    assert probe.get('errno') == errno.ECONNREFUSED, (
        'monitoring port %d accepts TCP connections without a usable TLS context: %s' % (port, probe)
    )


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
        _wait_for_marker(log_path, SECURITY_CONTEXT_MARKER, 1, 60)
        for attempt in range(1, 4):
            _wait_for_marker(log_path, RETRY_MARKER, attempt, 30)
            _assert_mon_port_refused(node.mon_port, 'retry_%d' % attempt)
        assert node.is_alive(), 'ydbd must keep running when the monitoring listener cannot start'
    finally:
        cluster.stop()


def test_valid_inline_pem_serves_https(tmp_path):
    cert_pem, key_pem = generate_selfsigned_cert('localhost')
    ca_path = os.path.join(str(tmp_path), 'mon_ca.pem')
    with open(ca_path, 'wb') as ca_file:
        ca_file.write(cert_pem)

    cluster = KiKiMR(_configurator((cert_pem + key_pem).decode('ascii')))
    cluster.prepare()
    node = cluster.nodes[1]
    # The harness only infers HTTPS from a certificate file; tell it the inline one is in effect.
    node.mon_uses_https = True
    try:
        assert cluster.start() is not None, 'cluster with an inline HTTPS monitoring certificate failed to start'
        response = requests.get('https://%s:%d/ping' % (node.host, node.mon_port), verify=ca_path, timeout=10)
        assert response.status_code == 200
    finally:
        cluster.stop()
