import socket
import platform
import multiprocessing
from distutils.version import LooseVersion

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

# save uname() on startup time: it is not so
# highly possible that the kernel will be
# changed in runtime, while calling uname()
# every time is a bit expensive
uname = tuple(platform.uname())
machine = platform.machine()
arch = platform.architecture()[0]
kernel = LooseVersion(uname[2]).version[:3]

AF_BRIDGE = getattr(socket, 'AF_BRIDGE', 7)
AF_NETLINK = getattr(socket, 'AF_NETLINK', 16)

data_plugins_pkgs = []
data_plugins_path = []

netns_path = ['/var/run/netns',
              '/var/run/docker/netns']
