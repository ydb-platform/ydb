'''
Basic network namespace management
==================================

Pyroute2 provides basic namespaces management support.
Here's a quick overview of typical netns tasks and
related pyroute2 tools.

Move an interface to a namespace
--------------------------------

Though this task is managed not via `netns` module, it
should be mentioned here as well. To move an interface
to a netns, one should provide IFLA_NET_NS_FD nla in
a set link RTNL request. The nla is an open FD number,
that refers to already created netns. The pyroute2
library provides also a possibility to specify not a
FD number, but a netns name as a string. In that case
the library will try to lookup the corresponding netns
in the standard location.

Create veth and move the peer to a netns with IPRoute::

    from pyroute2 import IPRoute
    ipr = IPRoute()
    ipr.link('add', ifname='v0p0', kind='veth', peer='v0p1')
    idx = ipr.link_lookup(ifname='v0p1')
    ipr.link('set', index=idx, net_ns_fd='netns_name')

Spawn a process within a netns
------------------------------

For that purpose one can use `NSPopen` API. It works just
as normal `Popen`, but starts a process within a netns.

Network namespace management
----------------------------

Please be aware, that in order to run system calls the
library uses `ctypes` module. It can fail on platforms
where SELinux is enforced. If the Python interpreter,
loading this module, dumps the core, one can check the
SELinux state with `getenforce` command.

'''

import ctypes
import ctypes.util
import errno
import io
import logging
import os
import os.path
import socket
from typing import Optional

from pyroute2 import config
from pyroute2.common import USE_DEFAULT_TIMEOUT, basestring, get_time
from pyroute2.process import ChildProcess, ChildProcessReturnValue

log = logging.getLogger(__name__)

try:
    file = file
except NameError:
    file = io.IOBase

# FIXME: arch reference
__NR = {
    'x86_': {'64bit': 308},
    'i386': {'32bit': 346},
    'i686': {'32bit': 346},
    'mips': {'32bit': 4344, '64bit': 5303},  # FIXME: NABI32?
    'loon': {'64bit': 268},
    'armv': {'32bit': 375},
    'aarc': {'32bit': 375, '64bit': 268},  # FIXME: EABI vs. OABI?
    'ppc6': {'64bit': 350},
    's390': {'64bit': 339},
    'loongarch64': {'64bit': 268},
    'risc': {'64bit': 268},
    'sw_6': {'64bit': 501},
}
__NR_setns = __NR.get(config.machine[:4], {}).get(config.arch, 308)

CLONE_NEWNET = 0x40000000
MNT_DETACH = 0x00000002
MS_BIND = 4096
MS_REC = 16384
MS_SHARED = 1 << 20
NETNS_RUN_DIR = '/var/run/netns'

__saved_ns = []
__libc = None


def _get_netnspath(name):
    netnspath = name
    dirname = os.path.dirname(name)
    if not dirname:
        netnspath = '%s/%s' % (NETNS_RUN_DIR, name)
    if hasattr(netnspath, 'encode'):
        netnspath = netnspath.encode('ascii')
    return netnspath


def _get_libc(libc=None):
    global __libc
    if libc is not None:
        return libc
    if __libc is None:
        __libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
    return __libc


def listnetns(nspath=None):
    '''
    List available network namespaces.
    '''
    if nspath:
        nsdir = nspath
    else:
        nsdir = NETNS_RUN_DIR
    try:
        return os.listdir(nsdir)
    except FileNotFoundError:
        return []


def _get_ns_by_inode(nspath=NETNS_RUN_DIR):
    '''
    Return a dict with inode as key and
    namespace name as value
    '''
    ns_by_dev_inode = {}
    for ns_name in listnetns(nspath=nspath):
        ns_path = os.path.join(nspath, ns_name)
        try:
            st = os.stat(ns_path)
        except FileNotFoundError:
            # The path disappeared from the FS while listing, ignore it
            continue
        if st.st_dev not in ns_by_dev_inode:
            ns_by_dev_inode[st.st_dev] = {}
        ns_by_dev_inode[st.st_dev][st.st_ino] = ns_name

    return ns_by_dev_inode


def ns_pids(nspath=NETNS_RUN_DIR):
    '''
    List pids in all netns

    If a pid is in a unknown netns do not return it
    '''
    result = {}
    ns_by_dev_inode = _get_ns_by_inode(nspath)

    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            st = os.stat(os.path.join('/proc', pid, 'ns', 'net'))
        except OSError as e:
            if e.errno in (errno.EACCES, errno.ENOENT):
                continue
            raise
        try:
            ns_name = ns_by_dev_inode[st.st_dev][st.st_ino]
        except KeyError:
            continue
        if ns_name not in result:
            result[ns_name] = []
        result[ns_name].append(int(pid))
    return result


def pid_to_ns(pid=1, nspath=NETNS_RUN_DIR):
    '''
    Return netns name which matches the given pid,
    None otherwise
    '''
    try:
        st = os.stat(os.path.join('/proc', str(pid), 'ns', 'net'))
        ns_by_dev_inode = _get_ns_by_inode(nspath)
        return ns_by_dev_inode[st.st_dev][st.st_ino]
    except OSError as e:
        if e.errno in (errno.EACCES, errno.ENOENT):
            return None
        raise
    except KeyError:
        return None


def _create(netns, libc=None, pid=None):
    libc = _get_libc(libc)
    netnspath = _get_netnspath(netns)
    netnsdir = os.path.dirname(netnspath)

    # init netnsdir
    try:
        os.mkdir(netnsdir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # this code is ported from iproute2
    done = False
    while libc.mount(b'', netnsdir, b'none', MS_SHARED | MS_REC, None) != 0:
        if done:
            raise OSError(ctypes.get_errno(), 'share rundir failed', netns)
        if (
            libc.mount(netnsdir, netnsdir, b'none', MS_BIND | MS_REC, None)
            != 0
        ):
            raise OSError(ctypes.get_errno(), 'mount rundir failed', netns)
        done = True

    # create mountpoint
    os.close(os.open(netnspath, os.O_RDONLY | os.O_CREAT | os.O_EXCL, 0))

    # unshare
    if pid is None:
        pid = 'self'
        if libc.unshare(CLONE_NEWNET) < 0:
            raise OSError(ctypes.get_errno(), 'unshare failed', netns)

    # bind the namespace
    if (
        libc.mount(
            '/proc/{}/ns/net'.format(pid).encode('utf-8'),
            netnspath,
            b'none',
            MS_BIND,
            None,
        )
        < 0
    ):
        raise OSError(ctypes.get_errno(), 'mount failed', netns)


@config.mock_if('mock_netns')
def create(netns, libc=None):
    '''
    Create a network namespace.
    '''
    proc = ChildProcess(target=_create, args=[netns, libc])
    proc.run()
    proc.communicate()
    proc.stop(kill=True)
    proc.close()


@config.mock_if('mock_netns')
def attach(netns, pid, libc=None):
    '''
    Attach the network namespace of the process `pid`
    to `netns` as if it were created with `create`.
    '''
    _create(netns, libc, pid)


@config.mock_if('mock_netns')
def remove(netns, libc=None):
    '''
    Remove a network namespace.
    '''
    libc = _get_libc(libc)
    netnspath = _get_netnspath(netns)
    libc.umount2(netnspath, MNT_DETACH)
    os.unlink(netnspath)


@config.mock_if('mock_netns')
def setns(netns, flags=os.O_CREAT, libc=None, fork=True):
    '''
    Set netns for the current process.

    The flags semantics is the same as for the `open(2)`
    call:

        - O_CREAT -- create netns, if doesn't exist
        - O_CREAT | O_EXCL -- create only if doesn't exist

    Note that "main" netns has no name. But you can access it with::

        setns('foo')  # move to netns foo
        setns('/proc/1/ns/net')  # go back to default netns

    See also `pushns()`/`popns()`/`dropns()`

    Changed in 0.5.1: the routine closes the ns fd if it's
    not provided via arguments.
    '''
    newfd = False
    libc = _get_libc(libc)
    if isinstance(netns, basestring):
        netnspath = _get_netnspath(netns)
        if os.path.basename(netns) in listnetns(os.path.dirname(netns)):
            if flags & (os.O_CREAT | os.O_EXCL) == (os.O_CREAT | os.O_EXCL):
                raise OSError(errno.EEXIST, 'netns exists', netns)
        else:
            if flags & os.O_CREAT:
                if fork:
                    create(netns, libc=libc)
                else:
                    _create(netns, libc=libc)
        nsfd = os.open(netnspath, os.O_RDONLY)
        newfd = True
    elif isinstance(netns, file):
        nsfd = netns.fileno()
    elif isinstance(netns, int):
        nsfd = netns
    else:
        raise RuntimeError('netns should be a string or an open fd')
    error = libc.syscall(__NR_setns, nsfd, CLONE_NEWNET)
    if newfd:
        os.close(nsfd)
    if error != 0:
        raise OSError(ctypes.get_errno(), 'failed to open netns', netns)


@config.mock_if('mock_netns')
def pushns(newns=None, libc=None):
    '''
    Save the current netns in order to return to it later. If newns is
    specified, change to it::

        # --> the script in the "main" netns
        netns.pushns("test")
        # --> changed to "test", the "main" is saved
        netns.popns()
        # --> "test" is dropped, back to the "main"
    '''
    global __saved_ns
    __saved_ns.append(os.open('/proc/self/ns/net', os.O_RDONLY))
    if newns is not None:
        setns(newns, libc=libc)


@config.mock_if('mock_netns')
def popns(libc=None):
    '''
    Restore the previously saved netns.
    '''
    global __saved_ns
    fd = __saved_ns.pop()
    try:
        setns(fd, libc=libc)
    except Exception:
        __saved_ns.append(fd)
        raise
    os.close(fd)


@config.mock_if('mock_netns')
def dropns(libc=None):
    '''
    Discard the last saved with `pushns()` namespace
    '''
    global __saved_ns
    fd = __saved_ns.pop()
    try:
        os.close(fd)
    except Exception:
        pass


def _create_socket_child(nsname, flags, family, socket_type, proto, libc=None):
    setns(nsname, flags=flags, libc=libc, fork=False)
    sock = socket.socket(family, socket_type, proto)
    return ChildProcessReturnValue(b'', [sock])


@config.mock_if('mock_netns')
def create_socket(
    netns: Optional[str] = None,
    family: int = socket.AF_INET,
    socket_type: int = socket.SOCK_STREAM,
    proto: int = 0,
    fileno: Optional[int] = None,
    flags: int = os.O_CREAT,
    libc: Optional[ctypes.CDLL] = None,
    timeout: int = USE_DEFAULT_TIMEOUT,
) -> socket.socket:
    if fileno is not None and netns is not None:
        raise TypeError('you can not specify both fileno and netns')
    if fileno is not None:
        return socket.socket(fileno=fileno)
    if netns is None:
        return socket.socket(family, socket_type, proto)
    if timeout == USE_DEFAULT_TIMEOUT:
        timeout = config.default_create_socket_timeout

    start_time = get_time()
    while get_time() - start_time < timeout:
        with ChildProcess(
            target=_create_socket_child,
            args=[netns, flags, family, socket_type, proto, libc],
        ) as proc:
            try:
                return socket.socket(
                    fileno=proc.get_fds(
                        timeout=config.default_communicate_timeout
                    )[0]
                )
            except TimeoutError:
                continue

    raise TimeoutError('could not start netns socket within timeout')
