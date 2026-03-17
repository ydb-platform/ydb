"""
Low level socket utilities.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong.
irmen@razorvine.net - http://www.razorvine.net/projects/Pyro
"""

import socket, os, errno, logging, time, sys
from Pyro4.errors import ConnectionClosedError, TimeoutError, CommunicationError

# Note: other interesting errnos are EPERM, ENOBUFS, EMFILE
# but it seems to me that all these signify an unrecoverable situation.
# So I didn't include them in de list of retryable errors.
ERRNO_RETRIES=[errno.EINTR, errno.EAGAIN, errno.EWOULDBLOCK]
if hasattr(errno, "WSAEINTR"):
    ERRNO_RETRIES.append(errno.WSAEINTR)
if hasattr(errno, "WSAEWOULDBLOCK"):
    ERRNO_RETRIES.append(errno.WSAEWOULDBLOCK)

ERRNO_BADF=[errno.EBADF]
if hasattr(errno, "WSAEBADF"):
    ERRNO_BADF.append(errno.WSAEBADF)

log=logging.getLogger("Pyro.socketutil")


def getIpAddress(hostname=None):
    """returns the IP address for the current, or another, hostname"""
    return socket.gethostbyname(hostname or socket.gethostname())


def getMyIpAddress(hostname=None, workaround127=False):
    """returns our own IP address. If you enable the workaround,
    it will use a little hack if the system reports our own ip address
    as being localhost (this is often the case on Linux)"""
    ip=getIpAddress(hostname)
    if ip.startswith("127.") and workaround127:
        ip=getInterfaceAddress("4.2.2.2")   # 'abuse' a level 3 DNS server
    return ip


def getInterfaceAddress(peer_ip_address):
    """tries to find the ip address of the interface that connects to a given host"""
    s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((peer_ip_address, 53))   # 53=dns
    ip=s.getsockname()[0]
    s.close()
    return ip


def __nextRetrydelay(delay):
    # first try a few very short delays,
    # if that doesn't work, increase by 0.1 sec every time
    if delay==0.0:
        return 0.001
    if delay==0.001:
        return 0.01
    return delay+0.1


if sys.version_info<(3, 0):
    EMPTY_BYTES=""
else:
    EMPTY_BYTES=bytes([])


def receiveData(sock, size):
    """Retrieve a given number of bytes from a socket.
    It is expected the socket is able to supply that number of bytes.
    If it isn't, an exception is raised (you will not get a zero length result
    or a result that is smaller than what you asked for). The partial data that
    has been received however is stored in the 'partialData' attribute of
    the exception object."""
    try:
        retrydelay=0.0
        msglen=0
        chunks=[]

        while True:
            try:
                while msglen<size:
                    # 60k buffer limit avoids problems on certain OSes like VMS, Windows
                    chunk=sock.recv(min(60000, size-msglen))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    msglen+=len(chunk)
                data=EMPTY_BYTES.join(chunks)
                del chunks
                if len(data)!=size:
                    err=ConnectionClosedError("receiving: not enough data")
                    err.partialData=data  # store the message that was received until now
                    raise err
                return data  # yay, complete
            except socket.timeout:
                raise TimeoutError("receiving: timeout")
            except socket.error:
                x=sys.exc_info()[1]
                err=getattr(x, "errno", x.args[0])
                if err not in ERRNO_RETRIES:
                    raise ConnectionClosedError("receiving: connection lost: "+str(x))
                time.sleep(0.00001+retrydelay)  # a slight delay to wait before retrying
                retrydelay=__nextRetrydelay(retrydelay)
    except socket.timeout:
        raise TimeoutError("receiving: timeout")


def sendData(sock, data):
    """
    Send some data over a socket.
    Some systems have problems with ``sendall()`` when the socket is in non-blocking mode.
    For instance, Mac OS X seems to be happy to throw EAGAIN errors too often.
    This function falls back to using a regular send loop if needed.
    """
    if sock.gettimeout() is None:
        # socket is in blocking mode, we can use sendall normally.
        while True:
            try:
                sock.sendall(data)
                return
            except socket.timeout:
                raise TimeoutError("sending: timeout")
            except socket.error:
                x=sys.exc_info()[1]
                raise ConnectionClosedError("sending: connection lost: "+str(x))
    else:
        # Socket is in non-blocking mode, use regular send loop.
        retrydelay=0.0
        while data:
            try:
                sent = sock.send(data)
                data = data[sent:]
            except socket.timeout:
                raise TimeoutError("sending: timeout")
            except socket.error:
                x=sys.exc_info()[1]
                err=getattr(x, "errno", x.args[0])
                if err not in ERRNO_RETRIES:
                    raise ConnectionClosedError("sending: connection lost: "+str(x))
                time.sleep(0.00001+retrydelay)  # a slight delay to wait before retrying
                retrydelay=__nextRetrydelay(retrydelay)


def createSocket(bind=None, connect=None, reuseaddr=True, keepalive=True, timeout=None, noinherit=False):
    """
    Create a socket. Default options are keepalives and reuseaddr.
    If 'bind' or 'connect' is a string, it is assumed a unix domain socket is requested.
    Otherwise, a normal tcp/ip socket is used.
    """
    for i in range(10):
        if timeout==0:
            timeout=None
        if bind and connect:
            raise ValueError("bind and connect cannot both be specified at the same time")
        family=socket.AF_INET
        if type(bind) is str or type(connect) is str:
            family=socket.AF_UNIX
        sock=socket.socket(family, socket.SOCK_STREAM)
        if reuseaddr:
            setReuseAddr(sock)
        if bind:
            if type(bind) is tuple and bind[1]==0:
                bindOnUnusedPort(sock, bind[0])
            else:
                sock.bind(bind)
            try:
                sock.listen(1024)
            except Exception:
                pass  # jython sometimes raises errors here
        if connect:
            sock.connect(connect)
            # Check what we really connected to not ourselves
            if sock.getsockname() != sock.getpeername():
                break
        else:
            break
    if keepalive:
        setKeepalive(sock)
    if noinherit:
        setNoInherit(sock)
    sock.settimeout(timeout)
    return sock


def createBroadcastSocket(bind=None, reuseaddr=True, timeout=None):
    """Create a udp broadcast socket."""
    if timeout==0:
        timeout=None
    sock=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    if reuseaddr:
        setReuseAddr(sock)
    if timeout is None:
        sock.settimeout(None)
    else:
        if bind and os.name=="java":
            # Jython has a problem with timeouts on udp sockets, see http://bugs.jython.org/issue1018
            log.warn("not setting timeout on broadcast socket due to Jython issue 1018")
        else:
            sock.settimeout(timeout)
    if bind:
        host,port=bind
        host=host or ""
        if port==0:
            bindOnUnusedPort(sock, host)
        else:
            sock.bind((host,port))
    return sock


def setReuseAddr(sock):
    """sets the SO_REUSEADDR option on the socket, if possible."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception:
        pass


def setKeepalive(sock):
    """sets the SO_KEEPALIVE option on the socket, if possible."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except Exception:
        pass

try:
    import fcntl

    def setNoInherit(sock):
        """Mark the given socket fd as non-inheritable to child processes"""
        fd = sock.fileno()
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

except ImportError:
    # no fcntl available, try the windows version
    try:
        from ctypes import windll, WinError

        def setNoInherit(sock):
            """Mark the given socket fd as non-inheritable to child processes"""
            if not windll.kernel32.SetHandleInformation(sock.fileno(), 1, 0):
                raise WinError()

    except ImportError:
        # nothing available, define a dummy function
        def setNoInherit(sock):
            """Mark the given socket fd as non-inheritable to child processes (dummy)"""
            pass


class SocketConnection(object):
    """A wrapper class for plain sockets, containing various methods such as :meth:`send` and :meth:`recv`"""
    __slots__=["sock", "objectId"]

    def __init__(self, sock, objectId=None):
        self.sock=sock
        self.objectId=objectId

    def __del__(self):
        self.close()

    def send(self, data):
        sendData(self.sock, data)

    def recv(self, size):
        return receiveData(self.sock, size)

    def close(self):
        self.sock.close()

    def fileno(self):
        return self.sock.fileno()

    def setTimeout(self, timeout):
        self.sock.settimeout(timeout)

    def getTimeout(self):
        return self.sock.gettimeout()
    timeout=property(getTimeout, setTimeout)


def findUnusedPort(family=socket.AF_INET, socktype=socket.SOCK_STREAM):
    """Returns an unused port that should be suitable for binding.
    This code is copied from the stdlib's test.test_support module."""
    tempsock = socket.socket(family, socktype)
    port = bindOnUnusedPort(tempsock)
    tempsock.close()
    del tempsock
    return port


def bindOnUnusedPort(sock, host='localhost'):
    """Bind the socket to a free port and return the port number.
    This code is based on the code in the stdlib's test.test_support module."""
    if os.name!="java" and sock.family == socket.AF_INET and sock.type == socket.SOCK_STREAM:
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    sock.bind((host, 0))
    if os.name=="java":
        try:
            sock.listen(100)  # otherwise jython always just returns 0 for the port
        except Exception:
            pass  # jython sometimes throws errors here
    port = sock.getsockname()[1]
    return port
