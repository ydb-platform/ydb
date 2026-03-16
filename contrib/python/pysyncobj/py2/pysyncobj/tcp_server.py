import socket

from .poller import POLL_EVENT_TYPE
from .tcp_connection import TcpConnection, _getAddrType


class SERVER_STATE:
    UNBINDED = 0,
    BINDED = 1


class TcpServer(object):

    def __init__(
            self, poller, host, port, onNewConnection,
            sendBufferSize = 2 ** 13,
            recvBufferSize = 2 ** 13,
            connectionTimeout = 3.5,
            keepalive = None,
    ):
        self.__poller = poller
        self.__host = host
        self.__port = int(port)
        self.__hostAddrType = _getAddrType(host)
        self.__sendBufferSize = sendBufferSize
        self.__recvBufferSize = recvBufferSize
        self.__socket = None
        self.__fileno = None
        self.__keepalive = keepalive
        self.__state = SERVER_STATE.UNBINDED
        self.__onNewConnectionCallback = onNewConnection
        self.__connectionTimeout = connectionTimeout

    def bind(self):
        self.__socket = socket.socket(self.__hostAddrType, socket.SOCK_STREAM)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize)
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__socket.setblocking(0)
        self.__socket.bind((self.__host, self.__port))
        self.__socket.listen(5)
        self.__fileno = self.__socket.fileno()
        self.__poller.subscribe(self.__fileno,
                                self.__onNewConnection,
                                POLL_EVENT_TYPE.READ | POLL_EVENT_TYPE.ERROR)
        self.__state = SERVER_STATE.BINDED

    def unbind(self):
        self.__state = SERVER_STATE.UNBINDED
        if self.__fileno is not None:
            self.__poller.unsubscribe(self.__fileno)
            self.__fileno = None
        if self.__socket is not None:
            self.__socket.close()

    def __onNewConnection(self, descr, event):
        if event & POLL_EVENT_TYPE.READ:
            try:
                sock, addr = self.__socket.accept()
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setblocking(0)
                conn = TcpConnection(
                    poller=self.__poller,
                    socket=sock,
                    timeout=self.__connectionTimeout,
                    sendBufferSize=self.__sendBufferSize,
                    recvBufferSize=self.__recvBufferSize,
                    keepalive=self.__keepalive,
                )
                self.__onNewConnectionCallback(conn)
            except socket.error as e:
                if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                    self.unbind()
                    return

        if event & POLL_EVENT_TYPE.ERROR:
            self.unbind()
            return
