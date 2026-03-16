"""
Socket server based on gevent library.

1. Socket listening blocks only current greenlet, not whole thread
2. Each worker spawned in it's own greenlet

It is able to easily handle 10000+ concurrent connections on all major platforms
(Windows, Linux, BSD).

Pyro - Python Remote Objects.  Copyright by Irmen de Jong.
irmen@razorvine.net - http://www.razorvine.net/python/Pyro
"""

import socket, sys, logging
from Pyro4.socketutil import SocketConnection, createSocket, ERRNO_RETRIES, ERRNO_BADF
from Pyro4.errors import ConnectionClosedError, PyroError
import Pyro4

log=logging.getLogger("Pyro.socketserver.gevent")

class SocketServer_Gevent(object):
    """transport server for socket connections, select/poll loop multiplex version."""
    def init(self, callbackObject, host, port, unixsocket=None):
        try:
            import gevent
            import gevent.server
            self.gevent = gevent
        except ImportError:
            raise NotImplementedError("gevent-based server is not supported: gevent module not found")
        log.info("starting select/poll socketserver")
        self.sock=None
        if unixsocket is not None:
            bind = unixsocket
        else:
            bind = (host, port)
        self.server=None
        self.sock=createSocket(bind=bind, timeout=Pyro4.config.COMMTIMEOUT)
        self.sock.setblocking(0)
        self.clients=[]
        self.callback=callbackObject
        if isinstance(bind, (list, tuple)):
            sockaddr=self.sock.getsockname()
            if sockaddr[0].startswith("127."):
                if host is None or host.lower()!="localhost" and not host.startswith("127."):
                    log.warn("weird DNS setup: %s resolves to localhost (127.x.x.x)",host)
            host=host or sockaddr[0]
            port=port or sockaddr[1]
            self.locationStr="%s:%d" % (host,port)
        else:
            self.locationStr='./u:%s' % (unixsocket, )
    def __del__(self):
        if hasattr(self, 'server') and self.server:
            self.server.stop()
        if hasattr(self, 'sock') and self.sock is not None:
            self.sock.close()
            self.sock=None

    def loop(self, loopCondition=lambda: True):
        self.loopCondition = loopCondition
        self.server = self.gevent.server.StreamServer(self.sock, handle=self.handleConnection)
        self.server.serve_forever()
    def handleConnection(self, csock, caddr):
        try:
            log.debug("connection from %s",caddr)
            if Pyro4.config.COMMTIMEOUT:
                csock.settimeout(Pyro4.config.COMMTIMEOUT)
        except socket.error:
            x=sys.exc_info()[1]
            err=getattr(x,"errno",x.args[0])
            if err in ERRNO_RETRIES:
                # just ignore this error for now and continue
                log.warn("accept() failed errno=%d, shouldn't happen", err)
                return None
            if err in ERRNO_BADF:
                # our server socket got destroyed
                log.info("server socket was closed, stopping requestloop")
                self.server.stop()
                return
            raise
        try:
            conn=SocketConnection(csock)
            if not self.callback._handshake(conn):
                return conn
        except (socket.error, PyroError):
            x=sys.exc_info()[1]
            log.warn("error during connect: %s",x)
            csock.close()
            return

        while self.sock is not None:
            try:
                self.callback.handleRequest(conn)
            except (socket.error,ConnectionClosedError):
                conn.close()
                break

        return None
    def close(self):
        log.debug("closing socketserver")
        if self.server:
            self.server.stop()
        if self.sock:
            self.sock.close()
        self.sock=None

    def fileno(self):
        return self.sock.fileno()
    def sockets(self):
        socks=[self.sock]
        return socks

    def wakeup(self):
        """bit of a hack to trigger a blocking server to get out of the loop, useful at clean shutdowns"""
        try:
            if sys.version_info<(3,0):
                self.sock.send("!"*16)
            else:
                self.sock.send(bytes([1]*16))
        except socket.error:
            pass
