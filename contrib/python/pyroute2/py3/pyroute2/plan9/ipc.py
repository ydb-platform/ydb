import collections
import multiprocessing
from socket import socketpair

from pyroute2.netlink.nlsocket import NetlinkSocket
from pyroute2.plan9.client import Plan9ClientSocket

IPCSocketPair = collections.namedtuple('IPCSocketPair', ('server', 'client'))


class IPCSocket(NetlinkSocket):

    def setup_socket(self):
        # create socket pair
        sp = IPCSocketPair(*socketpair())
        # start the server
        self.socket = sp
        self.p9server = multiprocessing.Process(target=self.ipc_server)
        self.p9server.daemon = True
        self.p9server.start()
        # create and init the client
        self.p9client = Plan9ClientSocket(use_socket=sp.client)
        self.p9client.init()
        return sp

    def ipc_server(self):
        raise NotImplementedError()

    def recv(self, buffersize, flags=0):
        ret = self.p9client.call(
            fid=self.p9client.fid('call'),
            fname='recv',
            kwarg={'buffersize': buffersize, 'flags': flags},
        )
        return ret['data']

    def send(self, data, flags=0):
        return self.p9client.call(
            fid=self.p9client.fid('call'),
            fname='send',
            kwarg={'flags': flags},
            data=data,
        )

    def bind(self):
        return self.p9client.call(fid=self.p9client.fid('call'), fname='bind')

    def close(self):
        self.socket.client.close()
        self.socket.server.close()
        self.p9server.wait()
