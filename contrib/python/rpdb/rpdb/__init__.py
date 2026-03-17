"""Remote Python Debugger (pdb wrapper)."""

__author__ = "Bertrand Janin <b@janin.com>"
__version__ = "0.1.6"

from contextlib import contextmanager
import pdb
import socket
import threading
import signal
import sys
import traceback
import os
import typing as t
from functools import partial

DEFAULT_ADDR = "127.0.0.1"
DEFAULT_PORT = 4444


def get_default_address():
    return os.environ.get("PYTHON_RPDB_ADDRESS", DEFAULT_ADDR)


def get_default_port():
    return int(os.environ.get("PYTHON_RPDB_PORT", DEFAULT_PORT))


def safe_print(msg):
    # Writes to stdout are forbidden in mod_wsgi environments
    try:
        sys.stderr.write("[rpdb] " + msg + "\n")
    except IOError:
        pass


# https://github.com/gotcha/ipdb/blob/400e37c56c9772fdc4c04ddb29d8a4a20568fb1a/ipdb/__main__.py#L233-L246
@contextmanager
def launch_ipdb_on_exception():
    try:
        yield
    except Exception:
        post_mortem()
    finally:
        pass


# iex is a concise alias
iex = launch_ipdb_on_exception()


def ipython_available():
    try:
        from IPython.core.debugger import Pdb

        return True
    except ImportError:
        return False


def get_debugger_class():
    # TODO shell = get_ipython() to detect if we are in ipython
    # from IPython.terminal.debugger import TerminalPdb

    debugger_base = pdb.Pdb

    if ipython_available():
        from IPython.core.debugger import Pdb

        debugger_base = Pdb

    class Debugger(Rpdb, debugger_base):
        def __init__(self, addr=None, port=None):
            Rpdb.__init__(self, addr=addr, port=port, debugger_base=debugger_base)

    return Debugger


def shutdown_socket(sock: socket.socket):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass  # Socket is already closed
    finally:
        sock.close()


class FileObjectWrapper(object):
    """
    A wrapper class for file objects that provides access to both the wrapped file object and standard I/O streams.
    """

    def __init__(self, fileobject, stdio):
        self._obj = fileobject
        self._io = stdio

    def __getattr__(self, attr):
        if hasattr(self._obj, attr):
            attr = getattr(self._obj, attr)
        elif hasattr(self._io, attr):
            attr = getattr(self._io, attr)
        else:
            raise AttributeError("Attribute %s is not found" % attr)
        return attr


class Rpdb:
    def __init__(self, addr=None, port=None, debugger_base=t.Type[pdb.Pdb]):
        """Initialize the socket and initialize pdb."""

        self.debugger = debugger_base

        addr = addr or get_default_address()
        port = port or get_default_port()

        safe_print(f"attempting to bind {addr}:{port}")

        # Backup stdin and stdout before replacing them by the socket handle
        self.dup_stdout_fileno = os.dup(sys.stdout.fileno())
        self.dup_stdin_fileno = os.dup(sys.stdin.fileno())
        self.port = port

        # Open a 'reusable' socket to let the webapp reload on the same port
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.skt.bind((addr, port))
        self.skt.listen(1)

        safe_print("running on %s:%d\n" % self.skt.getsockname())

        (clientsocket, address) = self.skt.accept()
        self.clientsocket = clientsocket
        handle = clientsocket.makefile("rw")

        self.debugger.__init__(
            self,
            completekey="tab",
            stdin=handle,
            stdout=handle,
        )

        # overwrite the default stdout and stdin with the socket file handles
        # if this isn't done, any other interactive programs (like `interact` or ipy) will
        os.dup2(clientsocket.fileno(), sys.stdout.fileno())
        os.dup2(clientsocket.fileno(), sys.stdin.fileno())

        OCCUPIED.claim(port, sys.stdout)

    def shutdown(self):
        """Revert stdin and stdout, close the socket."""
        safe_print("shutting down\n")

        # restore original stdout and stdin since we are exiting debug mode
        os.dup2(self.dup_stdout_fileno, sys.stdout.fileno())
        os.dup2(self.dup_stdin_fileno, sys.stdin.fileno())

        # `shutdown` on the `skt` will trigger an error
        # if you don't `shutdown` the `clientsocket` socat & friends will hang
        # self.clientsocket.shutdown(socket.SHUT_RDWR)
        # self.clientsocket.close()
        shutdown_socket(self.clientsocket)

        # self.skt.shutdown(socket.SHUT_RDWR)
        # self.skt.close()
        shutdown_socket(self.skt)

        OCCUPIED.unclaim(self.port)

    def do_continue(self, arg):
        """Clean-up and do underlying continue."""

        try:
            return self.debugger.do_continue(self, arg)
        finally:
            self.shutdown()

    do_c = do_cont = do_continue

    def do_quit(self, arg):
        """Clean-up and do underlying quit."""
        try:
            return self.debugger.do_quit(self, arg)
        finally:
            self.shutdown()

    do_q = do_exit = do_quit

    def do_EOF(self, arg):
        """Clean-up and do underlying EOF."""
        try:
            return self.debugger.do_EOF(self, arg)
        finally:
            self.shutdown()

    # TODO best approach here would be to bind the interactive ipy session to the new pipes we setup
    # def do_interact(self, arg):
    #     ipshell = embed.InteractiveShellEmbed(
    #         config=self.shell.config,
    #         banner1="*interactive*",
    #         exit_msg="*exiting interactive console...*",
    #     )
    #     global_ns = self.curframe.f_globals
    #     ipshell(
    #         module=sys.modules.get(global_ns["__name__"], None),
    #         local_ns=self.curframe_locals,
    #     )


def set_trace(addr=None, port=None, frame=None):
    """Wrapper function to keep the same import x; x.set_trace() interface.

    We catch all the possible exceptions from pdb and cleanup.

    """
    try:
        debugger = get_debugger_class()(addr=addr, port=port)
    except socket.error as e:
        if OCCUPIED.is_claimed(port, sys.stdout):
            # rpdb is already on this port - good enough, let it go on:
            safe_print("(Recurrent rpdb invocation ignored)\n")
            return
        else:
            # Port occupied by something else.
            safe_print("Target port is already in use. Original error: %s\n" % e)
            return

    try:
        debugger.set_trace(frame or sys._getframe().f_back)
    except Exception:
        traceback.print_exc()


def _trap_handler(addr, port, signum, frame):
    set_trace(addr, port, frame=frame)


def handle_trap(addr=None, port=None):
    """Register rpdb as the SIGTRAP signal handler"""
    signal.signal(signal.SIGTRAP, partial(_trap_handler, addr, port))


def post_mortem(addr=None, port=None):
    type, value, tb = sys.exc_info()
    traceback.print_exc()

    debugger = get_debugger_class()(addr=addr, port=port)
    debugger.reset()
    debugger.interaction(None, tb)


class OccupiedPorts(object):
    """Maintain rpdb port versus stdin/out file handles.

    Provides the means to determine whether or not a collision binding to a
    particular port is with an already operating rpdb session.

    Determination is according to whether a file handle is equal to what is
    registered against the specified port.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.claims = {}

    def claim(self, port, handle):
        self.lock.acquire(True)
        self.claims[port] = id(handle)
        self.lock.release()

    def is_claimed(self, port, handle):
        self.lock.acquire(True)
        got = self.claims.get(port) == id(handle)
        self.lock.release()
        return got

    def unclaim(self, port):
        self.lock.acquire(True)
        self.claims.pop(port, None)
        self.lock.release()


# {port: sys.stdout} pairs to track recursive rpdb invocation on same port.
# This scheme doesn't interfere with recursive invocations on separate ports -
# useful, eg, for concurrently debugging separate threads.
OCCUPIED = OccupiedPorts()
