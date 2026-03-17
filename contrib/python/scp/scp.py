# scp.py
# Copyright (C) 2008 James Bardin <j.bardin@gmail.com>

"""
Utilities for sending files over ssh using the scp1 protocol.
"""

__version__ = '0.15.0'

import locale
import os
import re
from socket import timeout as SocketTimeout


SCP_COMMAND = b'scp'

PATH_TYPES = (str, bytes)

try:
    import pathlib
except ImportError:
    pathlib = None
else:
    PATH_TYPES += pathlib.PurePath,
try:
    PATH_TYPES += unicode,
except NameError:
    pass

try:
    from typing import IO, TYPE_CHECKING, AnyStr, Callable, Iterable, Optional, Tuple, Union

    if TYPE_CHECKING:
        import paramiko.transport

    # this is some magic to make sure pyright doesn't get too confused with pathlib potentially being a nullable variable
    import pathlib
    PurePath = pathlib.PurePath

    PathTypes = Union[str, bytes, PurePath]
except ImportError:
    pass


# this is quote from the shlex module, added in py3.3
_find_unsafe = re.compile(br'[^\w@%+=:,./~-]').search

def _sh_quote(s):
    """Return a shell-escaped version of the string `s`."""
    if not s:
        return b""
    if _find_unsafe(s) is None:
        return s

    # use single quotes, and put single quotes into double quotes
    # the string $'b is then quoted as '$'"'"'b'
    return b"'" + s.replace(b"'", b"'\"'\"'") + b"'"


# Unicode conversion functions; assume UTF-8

def asbytes(s):
    """Turns unicode into bytes, if needed.

    Assumes UTF-8.
    """
    if isinstance(s, bytes):
        return s
    elif pathlib and isinstance(s, pathlib.PurePath):
        return bytes(s)
    else:
        return s.encode('utf-8')


def asunicode(s):
    """Turns bytes into unicode, if needed.

    Uses UTF-8.
    """
    if isinstance(s, bytes):
        return s.decode('utf-8', 'replace')
    else:
        return s


# os.path.sep is unicode on Python 3, no matter the platform
bytes_sep = asbytes(os.path.sep)


# Unicode conversion function for Windows
# Used to convert local paths if the local machine is Windows

def asunicode_win(s):
    """Turns bytes into unicode, if needed.
    """
    if isinstance(s, bytes):
        return s.decode(locale.getpreferredencoding())
    else:
        return s


class SCPClient(object):
    """
    An scp1 implementation, compatible with openssh scp.
    Raises SCPException for all transport related errors. Local filesystem
    and OS errors pass through.

    Main public methods are .put and .get
    The get method is controlled by the remote scp instance, and behaves
    accordingly. This means that symlinks are resolved, and the transfer is
    halted after too many levels of symlinks are detected.
    The put method uses os.walk for recursion, and sends files accordingly.
    Since scp doesn't support symlinks, we send file symlinks as the file
    (matching scp behaviour), but we make no attempt at symlinked directories.
    """
    def __init__(self, transport, buff_size=16384, socket_timeout=10.0,
                 progress=None, progress4=None, sanitize=_sh_quote, limit_bw=None):
        # type: (paramiko.transport.Transport, int, float, Optional[Callable[[bytes, int, int], None]], Optional[Callable[[bytes, int, int, Tuple[str, int]], None]], Callable[[bytes], bytes]) -> None
        """
        Create an scp1 client.

        @param transport: an existing paramiko L{Transport}
        @type transport: L{Transport}
        @param buff_size: size of the scp send buffer.
        @type buff_size: int
        @param socket_timeout: channel socket timeout in seconds
        @type socket_timeout: float
        @param progress: callback - called with (filename, size, sent) during
            transfers
        @param progress4: callback - called with (filename, size, sent, peername)
            during transfers. peername is a tuple contains (IP, PORT)
        @param sanitize: function - called with filename, should return
            safe or escaped string. Uses _sh_quote by default. Set to ``False``
            to disable.
        @type progress: function(string, int, int, tuple)
        @limit_bw: limits the bandwidth used on the connection in Kbps. Must be integer positive.
            the default value None does not limit the bandwidth.
        @type limit_bw: Optional[int]
        """
        self.transport = transport
        self.buff_size = buff_size
        self.socket_timeout = socket_timeout
        self.channel = None
        self.preserve_times = False
        if progress is not None and progress4 is not None:
            raise TypeError("You may only set one of progress, progress4")
        elif progress4 is not None:
            self._progress = progress4
        elif progress is not None:
            self._progress = lambda *a: progress(*a[:3])
        else:
            self._progress = None
        self._recv_dir = b''
        self._depth = 0
        self._rename = False
        self._utime = None
        if sanitize is False:
            self.sanitize = lambda x: x
        else:
            self.sanitize = sanitize
        self._dirtimes = {}
        self.peername = self.transport.getpeername()
        self.scp_command = SCP_COMMAND
        if limit_bw:
            self.scp_command += b' -l %d' % limit_bw

    def __enter__(self):
        self.channel = self._open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def put(self, files, remote_path=b'.',
            recursive=False, preserve_times=False):
        # type: (Union[PathTypes, Iterable[PathTypes]], Union[str, bytes], bool, bool) -> None
        """
        Transfer files and directories to remote host.

        @param files: A single path, or a list of paths to be transferred.
            recursive must be True to transfer directories.
        @type files: string OR list of strings
        @param remote_path: path in which to receive the files on the remote
            host. defaults to '.'
        @type remote_path: str
        @param recursive: transfer files and directories recursively
        @type recursive: bool
        @param preserve_times: preserve mtime and atime of transferred files
            and directories.
        @type preserve_times: bool
        """
        self.preserve_times = preserve_times
        self.channel = self._open()
        self._pushed = 0
        self.channel.settimeout(self.socket_timeout)
        scp_command = self.scp_command + b' ' + (b'-t ', b'-r -t ')[recursive]
        self.channel.exec_command(scp_command +
                                  self.sanitize(asbytes(remote_path)))
        self._recv_confirm()

        if isinstance(files, PATH_TYPES):
            files = [files]
        else:
            files = list(files)

        if recursive:
            self._send_recursive(files)
        else:
            self._send_files(files)

        self.close()

    def putfo(self, fl, remote_path, mode='0644', size=None):
        # type: (IO[AnyStr], Union[str, bytes], Union[str, bytes], Optional[int]) -> None
        """
        Transfer file-like object to remote host.

        @param fl: opened file or file-like object to copy
        @type fl: file-like object
        @param remote_path: full destination path
        @type remote_path: str
        @param mode: permissions (posix-style) for the uploaded file
        @type mode: str
        @param size: size of the file in bytes. If ``None``, the size will be
            computed using `seek()` and `tell()`.
        """
        if size is None:
            pos = fl.tell()
            fl.seek(0, os.SEEK_END)  # Seek to end
            size = fl.tell() - pos
            fl.seek(pos, os.SEEK_SET)  # Seek back

        self.channel = self._open()
        self.channel.settimeout(self.socket_timeout)
        self.channel.exec_command(self.scp_command + b' -t ' +
                                  self.sanitize(asbytes(remote_path)))
        self._recv_confirm()
        self._send_file(fl, remote_path, mode, size=size)
        self.close()

    def get(self, remote_path, local_path='',
            recursive=False, preserve_times=False):
        # type: (PathTypes, Union[str, bytes], bool, bool) -> None
        """
        Transfer files and directories from remote host to localhost.

        @param remote_path: path to retrieve from remote host. Note that
            wildcards will be escaped unless you changed the `sanitize`
            function.
        @type remote_path: str
        @param local_path: path in which to receive files locally
        @type local_path: str
        @param recursive: transfer files and directories recursively
        @type recursive: bool
        @param preserve_times: preserve mtime and atime of transferred files
            and directories.
        @type preserve_times: bool
        """
        if isinstance(remote_path, PATH_TYPES):
            remote_path = [remote_path]
        else:
            remote_path = list(remote_path)
        remote_path = [self.sanitize(asbytes(r)) for r in remote_path]
        self._recv_dir = local_path or os.getcwd()
        self._depth = 0
        self._rename = (len(remote_path) == 1 and
                        not os.path.isdir(os.path.abspath(local_path)))
        if len(remote_path) > 1:
            if not os.path.exists(self._recv_dir):
                raise SCPException("Local path '%s' does not exist" %
                                   asunicode(self._recv_dir))
            elif not os.path.isdir(self._recv_dir):
                raise SCPException("Local path '%s' is not a directory" %
                                   asunicode(self._recv_dir))
        rcsv = (b'', b' -r')[recursive]
        prsv = (b'', b' -p')[preserve_times]
        self.channel = self._open()
        self._pushed = 0
        self.channel.settimeout(self.socket_timeout)
        self.channel.exec_command(self.scp_command +
                                  rcsv +
                                  prsv +
                                  b" -f " +
                                  b' '.join(remote_path))
        self._recv_all()
        self.close()

    def _open(self):
        """open a scp channel"""
        if self.channel is None or self.channel.closed:
            self.channel = self.transport.open_session()

        return self.channel

    def close(self):
        """close scp channel"""
        if self.channel is not None:
            self.channel.close()
            self.channel = None

    def _read_stats(self, name):
        """return just the file stats needed for scp"""
        if os.name == 'nt':
            name = asunicode(name)
        stats = os.stat(name)
        mode = oct(stats.st_mode)[-4:]
        size = stats.st_size
        atime = int(stats.st_atime)
        mtime = int(stats.st_mtime)
        return (mode, size, mtime, atime)

    def _send_files(self, files):
        for name in files:
            (mode, size, mtime, atime) = self._read_stats(name)
            if self.preserve_times:
                self._send_time(mtime, atime)
            fl = open(name, 'rb')
            try:
                self._send_file(fl, name, mode, size)
            finally:
                fl.close()

    def _send_file(self, fl, name, mode, size):
        basename = asbytes(os.path.basename(name))
        # The protocol can't handle \n in the filename.
        # Quote them as the control sequence \^J for now,
        # which is how openssh handles it.
        self.channel.sendall(("C%s %d " % (mode, size)).encode('ascii') +
                             basename.replace(b'\n', b'\\^J') + b"\n")
        self._recv_confirm()
        file_pos = 0
        if self._progress:
            if size == 0:
                # avoid divide-by-zero
                self._progress(basename, 1, 1, self.peername)
            else:
                self._progress(basename, size, 0, self.peername)
        buff_size = self.buff_size
        chan = self.channel
        while file_pos < size:
            chan.sendall(fl.read(buff_size))
            file_pos = fl.tell()
            if self._progress:
                self._progress(basename, size, file_pos, self.peername)
        chan.sendall('\x00')
        self._recv_confirm()

    def _chdir(self, from_dir, to_dir):
        # Pop until we're one level up from our next push.
        # Push *once* into to_dir.
        # This is dependent on the depth-first traversal from os.walk

        # add path.sep to each when checking the prefix, so we can use
        # path.dirname after
        common = os.path.commonprefix([from_dir + bytes_sep,
                                       to_dir + bytes_sep])
        # now take the dirname, since commonprefix is character based,
        # and we either have a separator, or a partial name
        common = os.path.dirname(common)
        cur_dir = from_dir.rstrip(bytes_sep)
        while cur_dir != common:
            cur_dir = os.path.split(cur_dir)[0]
            self._send_popd()
        # now we're in our common base directory, so on
        self._send_pushd(to_dir)

    def _send_recursive(self, files):
        for base in files:
            base = asbytes(base)
            if not os.path.isdir(base):
                # filename mixed into the bunch
                self._send_files([base])
                continue
            last_dir = asbytes(base)
            for root, dirs, fls in os.walk(base):
                if not asbytes(root).endswith(b'/'):
                    self._chdir(last_dir, asbytes(root))
                self._send_files([os.path.join(root, f) for f in fls])
                last_dir = asbytes(root)
            # back out of the directory
            while self._pushed > 0:
                self._send_popd()

    def _send_pushd(self, directory):
        (mode, size, mtime, atime) = self._read_stats(directory)
        basename = asbytes(os.path.basename(directory))
        if self.preserve_times:
            self._send_time(mtime, atime)
        self.channel.sendall(('D%s 0 ' % mode).encode('ascii') +
                             basename.replace(b'\n', b'\\^J') + b'\n')
        self._recv_confirm()
        self._pushed += 1

    def _send_popd(self):
        self.channel.sendall('E\n')
        self._recv_confirm()
        self._pushed -= 1

    def _send_time(self, mtime, atime):
        self.channel.sendall(('T%d 0 %d 0\n' % (mtime, atime)).encode('ascii'))
        self._recv_confirm()

    def _recv_confirm(self):
        # read scp response
        msg = b''
        try:
            msg = self.channel.recv(512)
        except SocketTimeout:
            raise SCPException('Timeout waiting for scp response')
        # slice off the first byte, so this compare will work in py2 and py3
        if msg and msg[0:1] == b'\x00':
            return
        elif msg and msg[0:1] == b'\x01':
            raise SCPException(asunicode(msg[1:]))
        elif self.channel.recv_stderr_ready():
            msg = self.channel.recv_stderr(512)
            raise SCPException(asunicode(msg))
        elif not msg:
            raise SCPException('No response from server')
        else:
            raise SCPException('Invalid response from server', msg)

    def _recv_all(self):
        # loop over scp commands, and receive as necessary
        command = {b'C': self._recv_file,
                   b'T': self._set_time,
                   b'D': self._recv_pushd,
                   b'E': self._recv_popd}
        while not self.channel.closed:
            # wait for command as long as we're open
            self.channel.sendall('\x00')
            msg = self.channel.recv(1024)
            if not msg:  # chan closed while receiving
                break
            assert msg[-1:] == b'\n'
            msg = msg[:-1]
            code = msg[0:1]
            if code not in command:
                raise SCPException(asunicode(msg[1:]))
            command[code](msg[1:])
        # directory times can't be set until we're done writing files
        self._set_dirtimes()

    def _set_time(self, cmd):
        try:
            times = cmd.split(b' ')
            mtime = int(times[0])
            atime = int(times[2]) or mtime
        except:
            self.channel.send(b'\x01')
            raise SCPException('Bad time format')
        # save for later
        self._utime = (atime, mtime)

    def _recv_file(self, cmd):
        chan = self.channel
        parts = cmd.strip().split(b' ', 2)

        try:
            mode = int(parts[0], 8)
            size = int(parts[1])
            if self._rename:
                path = self._recv_dir
                self._rename = False
            elif os.name == 'nt':
                name = parts[2].decode('utf-8')
                assert not os.path.isabs(name)
                path = os.path.join(asunicode_win(self._recv_dir), name)
            else:
                name = parts[2]
                assert not os.path.isabs(name)
                path = os.path.join(asbytes(self._recv_dir), name)
        except:
            chan.send('\x01')
            chan.close()
            raise SCPException('Bad file format')

        try:
            file_hdl = open(path, 'wb')
        except IOError as e:
            chan.send(b'\x01' + str(e).encode('utf-8'))
            chan.close()
            raise

        if self._progress:
            if size == 0:
                # avoid divide-by-zero
                self._progress(path, 1, 1, self.peername)
            else:
                self._progress(path, size, 0, self.peername)
        buff_size = self.buff_size
        pos = 0
        chan.send(b'\x00')
        try:
            while pos < size:
                # we have to make sure we don't read the final byte
                if size - pos <= buff_size:
                    buff_size = size - pos
                data = chan.recv(buff_size)
                if not data:
                    raise SCPException("Underlying channel was closed")
                file_hdl.write(data)
                pos = file_hdl.tell()
                if self._progress:
                    self._progress(path, size, pos, self.peername)
            msg = chan.recv(512)
            if msg and msg[0:1] != b'\x00':
                raise SCPException(asunicode(msg[1:]))

            file_hdl.truncate()
            os.utime(path, self._utime)
            self._utime = None
            os.chmod(path, mode)
            # should we notify the other end?
        except SocketTimeout:
            chan.close()
            raise SCPException('Error receiving, socket.timeout')
        finally:
            file_hdl.close()
        # '\x00' confirmation sent in _recv_all

    def _recv_pushd(self, cmd):
        parts = cmd.split(b' ', 2)
        try:
            mode = int(parts[0], 8)
            if self._rename:
                path = self._recv_dir
                self._rename = False
            elif os.name == 'nt':
                name = parts[2].decode('utf-8')
                assert not os.path.isabs(name)
                path = os.path.join(asunicode_win(self._recv_dir), name)
                self._depth += 1
            else:
                name = parts[2]
                assert not os.path.isabs(name)
                path = os.path.join(asbytes(self._recv_dir), name)
                self._depth += 1
        except:
            self.channel.send(b'\x01')
            raise SCPException('Bad directory format')
        try:
            if not os.path.exists(path):
                os.mkdir(path, mode)
            elif os.path.isdir(path):
                os.chmod(path, mode)
            else:
                raise SCPException('%s: Not a directory' % path)
            self._dirtimes[path] = (self._utime)
            self._utime = None
            self._recv_dir = path
        except (OSError, SCPException) as e:
            self.channel.send(b'\x01' + asbytes(str(e)))
            raise

    def _recv_popd(self, *cmd):
        if self._depth > 0:
            self._depth -= 1
            self._recv_dir = os.path.split(self._recv_dir)[0]

    def _set_dirtimes(self):
        try:
            for d in self._dirtimes:
                os.utime(d, self._dirtimes[d])
        finally:
            self._dirtimes = {}


class SCPException(Exception):
    """SCP exception class"""
    pass


def put(transport, files, remote_path=b'.',
        recursive=False, preserve_times=False):
    # type: (paramiko.transport.Transport, Union[PathTypes, Iterable[PathTypes]], Union[str, bytes], bool, bool) -> None
    """
    Transfer files and directories to remote host.

    This is a convenience function that creates a SCPClient from the given
    transport and closes it at the end, useful for one-off transfers.

    @param files: A single path, or a list of paths to be transferred.
        recursive must be True to transfer directories.
    @type files: string OR list of strings
    @param remote_path: path in which to receive the files on the remote host.
        defaults to '.'
    @type remote_path: str
    @param recursive: transfer files and directories recursively
    @type recursive: bool
    @param preserve_times: preserve mtime and atime of transferred files and
        directories.
    @type preserve_times: bool
    """
    with SCPClient(transport) as client:
        client.put(files, remote_path, recursive, preserve_times)


def get(transport, remote_path, local_path='',
        recursive=False, preserve_times=False):
    # type: (paramiko.transport.Transport, PathTypes, Union[str, bytes], bool, bool) -> None
    """
    Transfer files and directories from remote host to localhost.

    This is a convenience function that creates a SCPClient from the given
    transport and closes it at the end, useful for one-off transfers.

    @param transport: an paramiko L{Transport}
    @type transport: L{Transport}
    @param remote_path: path to retrieve from remote host. Note that wildcards
        will be escaped unless you changed the `sanitize` function.
    @type remote_path: str
    @param local_path: path in which to receive files locally
    @type local_path: str
    @param recursive: transfer files and directories recursively
    @type recursive: bool
    @param preserve_times: preserve mtime and atime of transferred files
        and directories.
    @type preserve_times: bool
    """
    with SCPClient(transport) as client:
        client.get(remote_path, local_path, recursive, preserve_times)
