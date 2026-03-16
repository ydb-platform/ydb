"""A friendly Python SFTP interface."""
from __future__ import print_function

import os
from contextlib import contextmanager
import posixpath
import socket
from stat import S_IMODE, S_ISDIR, S_ISREG
import tempfile
import warnings

import paramiko
from paramiko import SSHException, AuthenticationException   # make available
from paramiko import AgentKey, RSAKey, DSSKey

from pysftp.exceptions import (CredentialException, ConnectionException,
                               HostKeysException)
from pysftp.helpers import (st_mode_to_int, WTCallbacks, path_advance,
                            path_retreat, reparent, walktree, cd, known_hosts)


__version__ = "0.2.9"
# pylint: disable = R0913,C0302


class CnOpts(object):   # pylint:disable=r0903
    '''additional connection options beyond authentication

    :ivar bool|str log: initial value: False -
        log connection/handshake details? If set to True,
        pysftp creates a temporary file and logs to that.  If set to a valid
        path and filename, pysftp logs to that.  The name of the logfile can
        be found at  ``.logfile``
    :ivar bool compression: initial value: False - Enables compression on the
        transport, if set to True.
    :ivar list|None ciphers: initial value: None -
        List of ciphers to use in order.
    :ivar paramiko.hostkeys.HostKeys|None hostkeys: HostKeys object to use for
        host key checking.
    :param filepath|None knownhosts: initial value: None - file to load
        hostkeys. If not specified, uses ~/.ssh/known_hosts
    :returns: (obj) CnOpts - A connection options object, used for passing
        extended options to the Connection
    :raises HostKeysException:
    '''
    def __init__(self, knownhosts=None):
        self.log = False
        self.compression = False
        self.ciphers = None
        if knownhosts is None:
            knownhosts = known_hosts()
        self.hostkeys = paramiko.hostkeys.HostKeys()
        try:
            self.hostkeys.load(knownhosts)
        except IOError:
            # can't find known_hosts in the standard place
            wmsg = "Failed to load HostKeys from %s.  " % knownhosts
            wmsg += "You will need to explicitly load HostKeys "
            wmsg += "(cnopts.hostkeys.load(filename)) or disable"
            wmsg += "HostKey checking (cnopts.hostkeys = None)."
            warnings.warn(wmsg, UserWarning)
        else:
            if len(self.hostkeys.items()) == 0:
                raise HostKeysException('No Host Keys Found')

    def get_hostkey(self, host):
        '''return the matching hostkey to use for verification for the host
        indicated or raise an SSHException'''
        kval = self.hostkeys.lookup(host)  # None|{keytype: PKey}
        if kval is None:
            raise SSHException("No hostkey for host %s found." % host)
        # return the pkey from the dict
        return list(kval.values())[0]


class Connection(object):   # pylint:disable=r0902,r0904
    """Connects and logs into the specified hostname.
    Arguments that are not given are guessed from the environment.

    :param str host:
        The Hostname or IP of the remote machine.
    :param str|None username: *Default: None* -
        Your username at the remote machine.
    :param str|obj|None private_key: *Default: None* -
        path to private key file(str) or paramiko.AgentKey
    :param str|None password: *Default: None* -
        Your password at the remote machine.
    :param int port: *Default: 22* -
        The SSH port of the remote machine.
    :param str|None private_key_pass: *Default: None* -
        password to use, if private_key is encrypted.
    :param list|None ciphers: *Deprecated* -
        see ``pysftp.CnOpts`` and ``cnopts`` parameter
    :param bool|str log: *Deprecated* -
        see ``pysftp.CnOpts`` and ``cnopts`` parameter
    :param None|CnOpts cnopts: *Default: None* - extra connection options
        set in a CnOpts object.
    :param str|None default_path: *Default: None* -
        set a default path upon connection.
    :returns: (obj) connection to the requested host
    :raises ConnectionException:
    :raises CredentialException:
    :raises SSHException:
    :raises AuthenticationException:
    :raises PasswordRequiredException:
    :raises HostKeysException:

    """

    def __init__(self, host, username=None, private_key=None, password=None,
                 port=22, private_key_pass=None, ciphers=None, log=False,
                 cnopts=None, default_path=None):
        # starting point for transport.connect options
        self._tconnect = {'username': username, 'password': password,
                          'hostkey': None, 'pkey': None}
        self._cnopts = cnopts or CnOpts()
        self._default_path = default_path
        # TODO: remove this if block and log param above in v0.3.0
        if log:
            wmsg = "log parameter is deprecated and will be remove in 0.3.0. "\
                   "Use cnopts param."
            warnings.warn(wmsg, DeprecationWarning)
            self._cnopts.log = log
        # TODO: remove this if block and log param above in v0.3.0
        if ciphers is not None:
            wmsg = "ciphers parameter is deprecated and will be remove in "\
                   "0.3.0. Use cnopts param."
            warnings.warn(wmsg, DeprecationWarning)
            self._cnopts.ciphers = ciphers
        # check that we have a hostkey to verify
        if self._cnopts.hostkeys is not None:
            self._tconnect['hostkey'] = self._cnopts.get_hostkey(host)

        self._sftp_live = False
        self._sftp = None
        self._set_username()
        self._set_logging()
        # Begin the SSH transport.
        self._transport = None
        self._start_transport(host, port)
        self._transport.use_compression(self._cnopts.compression)
        self._set_authentication(password, private_key, private_key_pass)
        self._transport.connect(**self._tconnect)

    def _set_authentication(self, password, private_key, private_key_pass):
        '''Authenticate the transport. prefer password if given'''
        if password is None:
            # Use Private Key.
            if not private_key:
                # Try to use default key.
                if os.path.exists(os.path.expanduser('~/.ssh/id_rsa')):
                    private_key = '~/.ssh/id_rsa'
                elif os.path.exists(os.path.expanduser('~/.ssh/id_dsa')):
                    private_key = '~/.ssh/id_dsa'
                else:
                    raise CredentialException("No password or key specified.")

            if isinstance(private_key, (AgentKey, RSAKey)):
                # use the paramiko agent or rsa key
                self._tconnect['pkey'] = private_key
            else:
                # isn't a paramiko AgentKey or RSAKey, try to build a
                # key from what we assume is a path to a key
                private_key_file = os.path.expanduser(private_key)
                try:  # try rsa
                    self._tconnect['pkey'] = RSAKey.from_private_key_file(
                        private_key_file, private_key_pass)
                except paramiko.SSHException:   # if it fails, try dss
                    # pylint:disable=r0204
                    self._tconnect['pkey'] = DSSKey.from_private_key_file(
                        private_key_file, private_key_pass)

    def _start_transport(self, host, port):
        '''start the transport and set the ciphers if specified.'''
        try:
            self._transport = paramiko.Transport((host, port))
            # Set security ciphers if set
            if self._cnopts.ciphers is not None:
                ciphers = self._cnopts.ciphers
                self._transport.get_security_options().ciphers = ciphers
        except (AttributeError, socket.gaierror):
            # couldn't connect
            raise ConnectionException(host, port)

    def _set_username(self):
        '''set the username for the connection. If not passed, then look to the
        environment.  Still nothing? Throw exception.'''
        if self._tconnect['username'] is None:
            self._tconnect['username'] = os.environ.get('LOGNAME', None)
            if self._tconnect['username'] is None:
                raise CredentialException('No username specified.')

    def _set_logging(self):
        '''set logging for connection'''
        if self._cnopts.log:
            if isinstance(self._cnopts.log, bool):
                # Log to a temporary file.
                fhnd, self._cnopts.log = tempfile.mkstemp('.txt', 'ssh-')
                os.close(fhnd)  # don't want os file descriptors open
            paramiko.util.log_to_file(self._cnopts.log)

    def _sftp_connect(self):
        """Establish the SFTP connection."""
        if not self._sftp_live:
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            if self._default_path is not None:
                # print("_default_path: [%s]" % self._default_path)
                self._sftp.chdir(self._default_path)
            self._sftp_live = True

    @property
    def pwd(self):
        '''return the current working directory

        :returns: (str) current working directory

        '''
        self._sftp_connect()
        return self._sftp.normalize('.')

    def get(self, remotepath, localpath=None, callback=None,
            preserve_mtime=False):
        """Copies a file between the remote host and the local host.

        :param str remotepath: the remote path and filename, source
        :param str localpath:
            the local path and filename to copy, destination. If not specified,
            file is copied to local current working directory
        :param callable callback:
            optional callback function (form: ``func(int, int)``) that accepts
            the bytes transferred so far and the total bytes to be transferred.
        :param bool preserve_mtime:
            *Default: False* - make the modification time(st_mtime) on the
            local file match the time on the remote. (st_atime can differ
            because stat'ing the localfile can/does update it's st_atime)

        :returns: None

        :raises: IOError

        """
        if not localpath:
            localpath = os.path.split(remotepath)[1]

        self._sftp_connect()
        if preserve_mtime:
            sftpattrs = self._sftp.stat(remotepath)

        self._sftp.get(remotepath, localpath, callback=callback)
        if preserve_mtime:
            os.utime(localpath, (sftpattrs.st_atime, sftpattrs.st_mtime))

    def get_d(self, remotedir, localdir, preserve_mtime=False):
        """get the contents of remotedir and write to locadir. (non-recursive)

        :param str remotedir: the remote directory to copy from (source)
        :param str localdir: the local directory to copy to (target)
        :param bool preserve_mtime: *Default: False* -
            preserve modification time on files

        :returns: None

        :raises:
        """
        self._sftp_connect()
        with self.cd(remotedir):
            for sattr in self._sftp.listdir_attr('.'):
                if S_ISREG(sattr.st_mode):
                    rname = sattr.filename
                    self.get(rname, reparent(localdir, rname),
                             preserve_mtime=preserve_mtime)

    def get_r(self, remotedir, localdir, preserve_mtime=False):
        """recursively copy remotedir structure to localdir

        :param str remotedir: the remote directory to copy from
        :param str localdir: the local directory to copy to
        :param bool preserve_mtime: *Default: False* -
            preserve modification time on files

        :returns: None

        :raises:

        """
        self._sftp_connect()
        wtcb = WTCallbacks()
        self.walktree(remotedir, wtcb.file_cb, wtcb.dir_cb, wtcb.unk_cb)
        # handle directories we recursed through
        for dname in wtcb.dlist:
            for subdir in path_advance(dname):
                try:
                    os.mkdir(reparent(localdir, subdir))
                    # force result to a list for setter,
                    wtcb.dlist = wtcb.dlist + [subdir, ]
                except OSError:     # dir exists
                    pass

        for fname in wtcb.flist:
            # they may have told us to start down farther, so we may not have
            # recursed through some, ensure local dir structure matches
            head, _ = os.path.split(fname)
            if head not in wtcb.dlist:
                for subdir in path_advance(head):
                    if subdir not in wtcb.dlist and subdir != '.':
                        os.mkdir(reparent(localdir, subdir))
                        wtcb.dlist = wtcb.dlist + [subdir, ]

            self.get(fname,
                     reparent(localdir, fname),
                     preserve_mtime=preserve_mtime)

    def getfo(self, remotepath, flo, callback=None):
        """Copy a remote file (remotepath) to a file-like object, flo.

        :param str remotepath: the remote path and filename, source
        :param flo: open file like object to write, destination.
        :param callable callback:
            optional callback function (form: ``func(int, int``)) that accepts
            the bytes transferred so far and the total bytes to be transferred.

        :returns: (int) the number of bytes written to the opened file object

        :raises: Any exception raised by operations will be passed through.

        """
        self._sftp_connect()
        return self._sftp.getfo(remotepath, flo, callback=callback)

    def put(self, localpath, remotepath=None, callback=None, confirm=True,
            preserve_mtime=False):
        """Copies a file between the local host and the remote host.

        :param str localpath: the local path and filename
        :param str remotepath:
            the remote path, else the remote :attr:`.pwd` and filename is used.
        :param callable callback:
            optional callback function (form: ``func(int, int``)) that accepts
            the bytes transferred so far and the total bytes to be transferred.
        :param bool confirm:
            whether to do a stat() on the file afterwards to confirm the file
            size
        :param bool preserve_mtime:
            *Default: False* - make the modification time(st_mtime) on the
            remote file match the time on the local. (st_atime can differ
            because stat'ing the localfile can/does update it's st_atime)

        :returns:
            (obj) SFTPAttributes containing attributes about the given file

        :raises IOError: if remotepath doesn't exist
        :raises OSError: if localpath doesn't exist

        """
        if not remotepath:
            remotepath = os.path.split(localpath)[1]
        self._sftp_connect()

        if preserve_mtime:
            local_stat = os.stat(localpath)
            times = (local_stat.st_atime, local_stat.st_mtime)

        sftpattrs = self._sftp.put(localpath, remotepath, callback=callback,
                                   confirm=confirm)
        if preserve_mtime:
            self._sftp.utime(remotepath, times)
            sftpattrs = self._sftp.stat(remotepath)

        return sftpattrs

    def put_d(self, localpath, remotepath, confirm=True, preserve_mtime=False):
        """Copies a local directory's contents to a remotepath

        :param str localpath: the local path to copy (source)
        :param str remotepath:
            the remote path to copy to (target)
        :param bool confirm:
            whether to do a stat() on the file afterwards to confirm the file
            size
        :param bool preserve_mtime:
            *Default: False* - make the modification time(st_mtime) on the
            remote file match the time on the local. (st_atime can differ
            because stat'ing the localfile can/does update it's st_atime)

        :returns: None

        :raises IOError: if remotepath doesn't exist
        :raises OSError: if localpath doesn't exist
        """
        self._sftp_connect()
        wtcb = WTCallbacks()
        cur_local_dir = os.getcwd()
        os.chdir(localpath)
        walktree('.', wtcb.file_cb, wtcb.dir_cb, wtcb.unk_cb,
                 recurse=False)
        for fname in wtcb.flist:
            src = os.path.join(localpath, fname)
            dest = reparent(remotepath, fname)
            # print('put', src, dest)
            self.put(src, dest, confirm=confirm, preserve_mtime=preserve_mtime)

        # restore local directory
        os.chdir(cur_local_dir)

    def put_r(self, localpath, remotepath, confirm=True, preserve_mtime=False):
        """Recursively copies a local directory's contents to a remotepath

        :param str localpath: the local path to copy (source)
        :param str remotepath:
            the remote path to copy to (target)
        :param bool confirm:
            whether to do a stat() on the file afterwards to confirm the file
            size
        :param bool preserve_mtime:
            *Default: False* - make the modification time(st_mtime) on the
            remote file match the time on the local. (st_atime can differ
            because stat'ing the localfile can/does update it's st_atime)

        :returns: None

        :raises IOError: if remotepath doesn't exist
        :raises OSError: if localpath doesn't exist
        """
        self._sftp_connect()
        wtcb = WTCallbacks()
        cur_local_dir = os.getcwd()
        os.chdir(localpath)
        walktree('.', wtcb.file_cb, wtcb.dir_cb, wtcb.unk_cb)
        # restore local directory
        os.chdir(cur_local_dir)
        for dname in wtcb.dlist:
            if dname != '.':
                pth = reparent(remotepath, dname)
                if not self.isdir(pth):
                    self.mkdir(pth)

        for fname in wtcb.flist:
            head, _ = os.path.split(fname)
            if head not in wtcb.dlist:
                for subdir in path_advance(head):
                    if subdir not in wtcb.dlist and subdir != '.':
                        self.mkdir(reparent(remotepath, subdir))
                        wtcb.dlist = wtcb.dlist + [subdir, ]
            src = os.path.join(localpath, fname)
            dest = reparent(remotepath, fname)
            # print('put', src, dest)
            self.put(src, dest, confirm=confirm, preserve_mtime=preserve_mtime)

    def putfo(self, flo, remotepath=None, file_size=0, callback=None,
              confirm=True):

        """Copies the contents of a file like object to remotepath.

        :param flo: a file-like object that supports .read()
        :param str remotepath: the remote path.
        :param int file_size:
            the size of flo, if not given the second param passed to the
            callback function will always be 0.
        :param callable callback:
            optional callback function (form: ``func(int, int``)) that accepts
            the bytes transferred so far and the total bytes to be transferred.
        :param bool confirm:
            whether to do a stat() on the file afterwards to confirm the file
            size

        :returns:
            (obj) SFTPAttributes containing attributes about the given file

        :raises: TypeError, if remotepath not specified, any underlying error

        """
        self._sftp_connect()
        return self._sftp.putfo(flo, remotepath, file_size=file_size,
                                callback=callback, confirm=confirm)

    def execute(self, command):
        """Execute the given commands on a remote machine.  The command is
        executed without regard to the remote :attr:`.pwd`.

        :param str command: the command to execute.

        :returns: (list of str) representing the results of the command

        :raises: Any exception raised by command will be passed through.

        """
        channel = self._transport.open_session()
        channel.exec_command(command)
        output = channel.makefile('rb', -1).readlines()
        if output:
            return output
        else:
            return channel.makefile_stderr('rb', -1).readlines()

    @contextmanager
    def cd(self, remotepath=None):  # pylint:disable=c0103
        """context manager that can change to a optionally specified remote
        directory and restores the old pwd on exit.

        :param str|None remotepath: *Default: None* -
            remotepath to temporarily make the current directory
        :returns: None
        :raises: IOError, if remote path doesn't exist
        """
        original_path = self.pwd
        try:
            if remotepath is not None:
                self.cwd(remotepath)
            yield
        finally:
            self.cwd(original_path)

    def chdir(self, remotepath):
        """change the current working directory on the remote

        :param str remotepath: the remote path to change to

        :returns: None

        :raises: IOError, if path does not exist

        """
        self._sftp_connect()
        self._sftp.chdir(remotepath)

    cwd = chdir     # synonym for chdir

    def chmod(self, remotepath, mode=777):
        """set the mode of a remotepath to mode, where mode is an integer
        representation of the octal mode to use.

        :param str remotepath: the remote path/file to modify
        :param int mode: *Default: 777* -
            int representation of octal mode for directory

        :returns: None

        :raises: IOError, if the file doesn't exist

        """
        self._sftp_connect()
        self._sftp.chmod(remotepath, mode=int(str(mode), 8))

    def chown(self, remotepath, uid=None, gid=None):
        """ set uid and/or gid on a remotepath, you may specify either or both.
        Unless you have **permission** to do this on the remote server, you
        will raise an IOError: 13 - permission denied

        :param str remotepath: the remote path/file to modify
        :param int uid: the user id to set on the remotepath
        :param int gid: the group id to set on the remotepath

        :returns: None

        :raises:
            IOError, if you don't have permission or the file doesn't exist

        """
        self._sftp_connect()
        if uid is None or gid is None:
            if uid is None and gid is None:  # short circuit if no change
                return
            rstat = self._sftp.stat(remotepath)
            if uid is None:
                uid = rstat.st_uid
            if gid is None:
                gid = rstat.st_gid

        self._sftp.chown(remotepath, uid=uid, gid=gid)

    def getcwd(self):
        """return the current working directory on the remote. This is a wrapper
        for paramiko's method and not to be confused with the SFTP command,
        cwd.

        :returns: (str) the current remote path. None, if not set.

        """
        self._sftp_connect()
        return self._sftp.getcwd()

    def listdir(self, remotepath='.'):
        """return a list of files/directories for the given remote path.
        Unlike, paramiko, the directory listing is sorted.

        :param str remotepath: path to list on the server

        :returns: (list of str) directory entries, sorted

        """
        self._sftp_connect()
        return sorted(self._sftp.listdir(remotepath))

    def listdir_attr(self, remotepath='.'):
        """return a list of SFTPAttribute objects of the files/directories for
        the given remote path. The list is in arbitrary order. It does not
        include the special entries '.' and '..'.

        The returned SFTPAttributes objects will each have an additional field:
        longname, which may contain a formatted string of the file's
        attributes, in unix format. The content of this string will depend on
        the SFTP server.

        :param str remotepath: path to list on the server

        :returns: (list of SFTPAttributes), sorted

        """
        self._sftp_connect()
        return sorted(self._sftp.listdir_attr(remotepath),
                      key=lambda attr: attr.filename)

    def mkdir(self, remotepath, mode=777):
        """Create a directory named remotepath with mode. On some systems,
        mode is ignored. Where it is used, the current umask value is first
        masked out.

        :param str remotepath: directory to create`
        :param int mode: *Default: 777* -
            int representation of octal mode for directory

        :returns: None

        """
        self._sftp_connect()
        self._sftp.mkdir(remotepath, mode=int(str(mode), 8))

    def normalize(self, remotepath):
        """Return the expanded path, w.r.t the server, of a given path.  This
        can be used to resolve symlinks or determine what the server believes
        to be the :attr:`.pwd`, by passing '.' as remotepath.

        :param str remotepath: path to be normalized

        :return: (str) normalized form of the given path

        :raises: IOError, if remotepath can't be resolved
        """
        self._sftp_connect()
        return self._sftp.normalize(remotepath)

    def isdir(self, remotepath):
        """return true, if remotepath is a directory

        :param str remotepath: the path to test

        :returns: (bool)

        """
        self._sftp_connect()
        try:
            result = S_ISDIR(self._sftp.stat(remotepath).st_mode)
        except IOError:     # no such file
            result = False
        return result

    def isfile(self, remotepath):
        """return true if remotepath is a file

        :param str remotepath: the path to test

        :returns: (bool)

        """
        self._sftp_connect()
        try:
            result = S_ISREG(self._sftp.stat(remotepath).st_mode)
        except IOError:     # no such file
            result = False
        return result

    def makedirs(self, remotedir, mode=777):
        """create all directories in remotedir as needed, setting their mode
        to mode, if created.

        If remotedir already exists, silently complete. If a regular file is
        in the way, raise an exception.

        :param str remotedir: the directory structure to create
        :param int mode: *Default: 777* -
            int representation of octal mode for directory

        :returns: None

        :raises: OSError

        """
        self._sftp_connect()
        if self.isdir(remotedir):
            pass

        elif self.isfile(remotedir):
            raise OSError("a file with the same name as the remotedir, "
                          "'%s', already exists." % remotedir)
        else:

            head, tail = os.path.split(remotedir)
            if head and not self.isdir(head):
                self.makedirs(head, mode)

            if tail:
                self.mkdir(remotedir, mode=mode)

    def readlink(self, remotelink):
        """Return the target of a symlink (shortcut).  The result will be
        an absolute pathname.

        :param str remotelink: remote path of the symlink

        :return: (str) absolute path to target

        """
        self._sftp_connect()
        return self._sftp.normalize(self._sftp.readlink(remotelink))

    def remove(self, remotefile):
        """remove the file @ remotefile, remotefile may include a path, if no
        path, then :attr:`.pwd` is used.  This method only works on files

        :param str remotefile: the remote file to delete

        :returns: None

        :raises: IOError

        """
        self._sftp_connect()
        self._sftp.remove(remotefile)

    unlink = remove     # synonym for remove

    def rmdir(self, remotepath):
        """remove remote directory

        :param str remotepath: the remote directory to remove

        :returns: None

        """
        self._sftp_connect()
        self._sftp.rmdir(remotepath)

    def rename(self, remote_src, remote_dest):
        """rename a file or directory on the remote host.

        :param str remote_src: the remote file/directory to rename

        :param str remote_dest: the remote file/directory to put it

        :returns: None

        :raises: IOError

        """
        self._sftp_connect()
        self._sftp.rename(remote_src, remote_dest)

    def stat(self, remotepath):
        """return information about file/directory for the given remote path

        :param str remotepath: path to stat

        :returns: (obj) SFTPAttributes

        """
        self._sftp_connect()
        return self._sftp.stat(remotepath)

    def lstat(self, remotepath):
        """return information about file/directory for the given remote path,
        without following symbolic links. Otherwise, the same as .stat()

        :param str remotepath: path to stat

        :returns: (obj) SFTPAttributes object

        """
        self._sftp_connect()
        return self._sftp.lstat(remotepath)

    def close(self):
        """Closes the connection and cleans up."""
        # Close SFTP Connection.
        if self._sftp_live:
            self._sftp.close()
            self._sftp_live = False
        # Close the SSH Transport.
        if self._transport:
            self._transport.close()
            self._transport = None
        # clean up any loggers
        if self._cnopts.log:
            # if handlers are active they hang around until the app exits
            # this closes and removes the handlers if in use at close
            import logging
            lgr = logging.getLogger("paramiko")
            if lgr:
                lgr.handlers = []

    def open(self, remote_file, mode='r', bufsize=-1):
        """Open a file on the remote server.

        See http://paramiko-docs.readthedocs.org/en/latest/api/sftp.html for
        details.

        :param str remote_file: name of the file to open.
        :param str mode:
            mode (Python-style) to open file (always assumed binary)
        :param int bufsize: *Default: -1* - desired buffering

        :returns: (obj) SFTPFile, a handle the remote open file

        :raises: IOError, if the file could not be opened.

        """
        self._sftp_connect()
        return self._sftp.open(remote_file, mode=mode, bufsize=bufsize)

    def exists(self, remotepath):
        """Test whether a remotepath exists.

        :param str remotepath: the remote path to verify

        :returns: (bool) True, if remotepath exists, else False

        """
        self._sftp_connect()
        try:
            self._sftp.stat(remotepath)
        except IOError:
            return False
        return True

    def lexists(self, remotepath):
        """Test whether a remotepath exists.  Returns True for broken symbolic
        links

        :param str remotepath: the remote path to verify

        :returns: (bool), True, if lexists, else False

        """
        self._sftp_connect()
        try:
            self._sftp.lstat(remotepath)
        except IOError:
            return False
        return True

    def symlink(self, remote_src, remote_dest):
        '''create a symlink for a remote file on the server

        :param str remote_src: path of original file
        :param str remote_dest: path of the created symlink

        :returns: None

        :raises:
            any underlying error, IOError if something already exists at
            remote_dest

        '''
        self._sftp_connect()
        self._sftp.symlink(remote_src, remote_dest)

    def truncate(self, remotepath, size):
        """Change the size of the file specified by path. Used to modify the
        size of the file, just like the truncate method on Python file objects.
        The new file size is confirmed and returned.

        :param str remotepath: remote file path to modify
        :param int|long size: the new file size

        :returns: (int) new size of file

        :raises: IOError, if file does not exist

        """
        self._sftp_connect()
        self._sftp.truncate(remotepath, size)
        return self._sftp.stat(remotepath).st_size

    def walktree(self, remotepath, fcallback, dcallback, ucallback,
                 recurse=True):
        '''recursively descend, depth first, the directory tree rooted at
        remotepath, calling discreet callback functions for each regular file,
        directory and unknown file type.

        :param str remotepath:
            root of remote directory to descend, use '.' to start at
            :attr:`.pwd`
        :param callable fcallback:
            callback function to invoke for a regular file.
            (form: ``func(str)``)
        :param callable dcallback:
            callback function to invoke for a directory. (form: ``func(str)``)
        :param callable ucallback:
            callback function to invoke for an unknown file type.
            (form: ``func(str)``)
        :param bool recurse: *Default: True* - should it recurse

        :returns: None

        :raises:

        '''
        self._sftp_connect()
        for entry in self.listdir(remotepath):
            pathname = posixpath.join(remotepath, entry)
            mode = self._sftp.stat(pathname).st_mode
            if S_ISDIR(mode):
                # It's a directory, call the dcallback function
                dcallback(pathname)
                if recurse:
                    # now, recurse into it
                    self.walktree(pathname, fcallback, dcallback, ucallback)
            elif S_ISREG(mode):
                # It's a file, call the fcallback function
                fcallback(pathname)
            else:
                # Unknown file type
                ucallback(pathname)

    @property
    def sftp_client(self):
        """give access to the underlying, connected paramiko SFTPClient object

        see http://paramiko-docs.readthedocs.org/en/latest/api/sftp.html

        :params: None

        :returns: (obj) the active SFTPClient object

        """
        self._sftp_connect()
        return self._sftp

    @property
    def active_ciphers(self):
        """Get tuple of currently used local and remote ciphers.

        :returns:
            (tuple of  str) currently used ciphers (local_cipher,
            remote_cipher)

        """
        return self._transport.local_cipher, self._transport.remote_cipher

    @property
    def active_compression(self):
        """Get tuple of currently used local and remote compression.

        :returns:
            (tuple of  str) currently used compression (local_compression,
            remote_compression)

        """
        localc = self._transport.local_compression
        remotec = self._transport.remote_compression
        return localc, remotec

    @property
    def security_options(self):
        """return the available security options recognized by paramiko.

        :returns:
            (obj) security preferences of the ssh transport. These are tuples
            of acceptable `.ciphers`, `.digests`, `.key_types`, and key
            exchange algorithms `.kex`, listed in order of preference.

        """

        return self._transport.get_security_options()

    @property
    def logfile(self):
        '''return the name of the file used for logging or False it not logging

        :returns: (str)logfile or (bool) False

        '''
        return self._cnopts.log

    @property
    def timeout(self):
        ''' (float|None) *Default: None* -
            get or set the underlying socket timeout for pending read/write
            ops.

        :returns:
            (float|None) seconds to wait for a pending read/write operation
            before raising socket.timeout, or None for no timeout
        '''
        self._sftp_connect()
        channel = self._sftp.get_channel()

        return channel.gettimeout()

    @timeout.setter
    def timeout(self, val):
        '''setter for timeout'''
        self._sftp_connect()
        channel = self._sftp.get_channel()
        channel.settimeout(val)

    @property
    def remote_server_key(self):
        '''return the remote server's key'''
        return self._transport.get_remote_server_key()

    def __del__(self):
        """Attempt to clean up if not explicitly closed."""
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, etype, value, traceback):
        self.close()
