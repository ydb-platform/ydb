import errno
import json
import shutil
import socket
import ssl
import subprocess

from pyroute2.netlink import nlmsg
from pyroute2.netlink.exceptions import NetlinkError


class probe_msg(nlmsg):
    '''
    Fake message type to represent network probe info.

    This is a prototype, the NLA layout is subject to change without
    notification.
    '''

    __slots__ = ()
    prefix = 'PROBE_'

    fields = (('family', 'B'), ('proto', 'B'), ('port', 'H'), ('dst_len', 'I'))

    nla_map = (
        ('PROBE_UNSPEC', 'none'),
        ('PROBE_KIND', 'asciiz'),
        ('PROBE_STDOUT', 'asciiz'),
        ('PROBE_STDERR', 'asciiz'),
        ('PROBE_SRC', 'asciiz'),
        ('PROBE_DST', 'asciiz'),
        ('PROBE_NUM', 'uint8'),
        ('PROBE_TIMEOUT', 'uint8'),
        ('PROBE_HOSTNAME', 'asciiz'),
        ('PROBE_SSL_VERIFY', 'uint8'),
        ('PROBE_SSL_VERSION', 'asciiz'),
        ('PROBE_SSL_CERT_JSON', 'asciiz'),
        ('PROBE_SSL_CERT_DER', 'cdata'),
    )


def probe_ping(msg):
    num = msg.get('num')
    timeout = msg.get('timeout')
    dst = msg.get('dst')
    kind = msg.get('kind')
    args = [shutil.which(kind), '-c', f'{num}', '-W', f'{timeout}', f'{dst}']
    if args[0] is None:
        raise NetlinkError(errno.ENOENT, 'probe not found')

    process = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        out, err = process.communicate(timeout=timeout)
        if out:
            msg['attrs'].append(['PROBE_STDOUT', out])
        if err:
            msg['attrs'].append(['PROBE_STDERR', err])
    except subprocess.TimeoutExpired:
        process.terminate()
        raise NetlinkError(errno.ETIMEDOUT, 'timeout expired')
    finally:
        process.stdout.close()
        process.stderr.close()
        return_code = process.wait()
    if return_code != 0:
        raise NetlinkError(errno.EHOSTUNREACH, 'probe failed')


def probe_tcp(msg, close=True):
    timeout = msg.get('timeout')
    dst = msg.get('dst')
    port = msg.get('port')
    connection = None
    try:
        connection = socket.create_connection((dst, port), timeout=timeout)
    except ConnectionRefusedError:
        raise NetlinkError(errno.ECONNREFUSED, 'connection refused')
    except TimeoutError:
        raise NetlinkError(errno.ETIMEDOUT, 'timeout expired')
    except Exception:
        raise NetlinkError(errno.ECOMM, 'probe failed')
    finally:
        if close and connection is not None:
            connection.close()
    return connection


def probe_ssl(msg):
    hostname = msg.get('hostname') or msg.get('dst')
    context = ssl.create_default_context()
    context.verify_mode = msg.get('ssl_verify', ssl.CERT_REQUIRED)
    with probe_tcp(msg, close=False) as connection:
        try:
            with context.wrap_socket(
                connection, server_hostname=hostname
            ) as ssl_wrap:
                version = ssl_wrap.version()
                peer_cert_json = ssl_wrap.getpeercert(binary_form=False)
                peer_cert_der = ssl_wrap.getpeercert(binary_form=True)
                if peer_cert_json is not None:
                    msg['attrs'].append(
                        ['PROBE_SSL_CERT_JSON', json.dumps(peer_cert_json)]
                    )
                if peer_cert_der is not None:
                    msg['attrs'].append(['PROBE_SSL_CERT_DER', peer_cert_der])
                if version is not None:
                    msg['attrs'].append(['PROBE_SSL_VERSION', version])
        except ssl.SSLError as e:
            code = errno.EPROTO
            if e.reason == 'UNSUPPORTED_PROTOCOL':
                code = errno.EPROTONOSUPPORT
            elif e.reason == 'CERTIFICATE_VERIFY_FAILED':
                code = errno.EACCES
            raise NetlinkError(code, e.strerror)
        except Exception:
            raise NetlinkError(errno.ECOMM, 'probe failed')


def proxy_newprobe(msg):
    kind = msg.get('kind')
    if kind.startswith('ping'):
        probe_ping(msg)
    elif kind == 'tcp':
        probe_tcp(msg)
    elif kind == 'ssl':
        probe_ssl(msg)
    else:
        raise NetlinkError(errno.ENOTSUP, 'probe type not supported')
    msg.reset()
    msg.encode()
    return msg.data
