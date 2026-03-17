import multiprocessing
import platform
import re
import signal
import socket
from typing import Literal, Union

metric_type = Literal['c', 'g', 'ms']


class LocalMock:

    def __init__(self):
        self.call_args_list = []

    def __call__(self, *argv, **kwarg):
        self.call_args_list.append((argv, kwarg))
        return self

    def close(self) -> None:
        pass

    def setsockopt(self, level: int, optname: int, value: int) -> None:
        pass

    def fileno(self) -> None:
        pass

    def put(
        self, name: str, value: Union[int, str], kind: metric_type
    ) -> None:
        pass

    def commit(self) -> None:
        pass

    def incr(self, name: str, value: int = 1) -> None:
        pass


def mock_if(name):
    def decorator(func):
        local_mock = LocalMock()

        def wrapper(*argv, **kwarg):
            if globals().get(name, False):
                local_mock(*argv, **kwarg)
                return local_mock
            return func(*argv, **kwarg)

        wrapper.__doc__ = func.__doc__
        wrapper.__name__ = func.__name__
        return wrapper

    return decorator


kernel_version_re = re.compile('^[0-9.]+')


def parse_kernel_version(kernel_name):
    match_obj = kernel_version_re.match(kernel_name)
    if match_obj is None:
        return []
    return [int(x) for x in kernel_name[0 : match_obj.end()].split('.') if x]


SocketBase = socket.socket
MpPipe = multiprocessing.Pipe
MpQueue = multiprocessing.Queue
MpProcess = multiprocessing.Process
ipdb_nl_async = True
nlm_generator = False
nla_via_getattr = False
async_qsize = 4096
commit_barrier = 0
gc_timeout = 60
db_transaction_limit = 1
cache_expire = 60
telemetry = None
child_process_mode = 'fork'
# Disable signal in 'mp' mode of child_process_mode
disable_mp_signal = False
force_gc = False
signal_stop_remote = None
if hasattr(signal, 'SIGUSR1'):
    signal_stop_remote = signal.SIGUSR1

mock_netlink = False
mock_netns = False
nlsocket_thread_safe = True

# save uname() on startup time: it is not so
# highly possible that the kernel will be
# changed in runtime, while calling uname()
# every time is a bit expensive
uname = tuple(platform.uname())
machine = platform.machine()
arch = platform.architecture()[0]
kernel = parse_kernel_version(uname[2])

default_create_socket_timeout = 10
default_communicate_timeout = 2

AF_BRIDGE = getattr(socket, 'AF_BRIDGE', 7)
AF_NETLINK = getattr(socket, 'AF_NETLINK', 16)

data_plugins_pkgs = []
data_plugins_path = []

netns_path = ['/var/run/netns', '/var/run/docker/netns']

entry_points_aliases = {
    'pyroute2.netlink.exceptions': 'pyroute2.netlink.exceptions'
}
