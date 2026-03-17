# Copyright (C) 2006-2008 Jelmer Vernooij <jelmer@jelmer.uk>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
"""Python bindings for Subversion."""

__author__ = "Jelmer Vernooij <jelmer@jelmer.uk>"

try:
    from SocketServer import StreamRequestHandler, TCPServer
except ImportError:
    from socketserver import StreamRequestHandler, TCPServer
import base64
import os
import socket
import subprocess
from errno import EPIPE
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from subvertpy import (
    ERR_RA_SVN_UNKNOWN_CMD,
    ERR_UNSUPPORTED_FEATURE,
    NODE_DIR,
    NODE_FILE,
    NODE_UNKNOWN,
    NODE_NONE,
    SubversionException,
    properties,
    )
from subvertpy.delta import (
    pack_svndiff0_window,
    unpack_svndiff0,
    SVNDIFF0_HEADER,
    )
from subvertpy.marshall import (
    NeedMoreData,
    literal,
    marshall,
    unmarshall,
    )
from subvertpy.ra import (
    DIRENT_CREATED_REV,
    DIRENT_HAS_PROPS,
    DIRENT_KIND,
    DIRENT_LAST_AUTHOR,
    DIRENT_SIZE,
    DIRENT_TIME,
    )
from subvertpy.server import (
    generate_random_id,
    )


class SSHSubprocess(object):
    """A socket-like object that talks to an ssh subprocess via pipes."""

    __slots__ = ('proc')

    def __init__(self, proc):
        self.proc = proc

    def send(self, data):
        return os.write(self.proc.stdin.fileno(), data)

    def recv(self, count):
        return os.read(self.proc.stdout.fileno(), count)

    def close(self):
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.proc.wait()

    def get_filelike_channels(self):
        return (self.proc.stdout, self.proc.stdin)


class SSHVendor(object):

    def connect_ssh(self, username, password, host, port, command):
        args = ['ssh', '-x']
        if port is not None:
            args.extend(['-p', str(port)])
        if username is not None:
            host = "%s@%s" % (username, host)
        args.append(host)
        proc = subprocess.Popen(args + command,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        return SSHSubprocess(proc)


# Can be overridden by users
get_ssh_vendor = SSHVendor


class SVNConnection(object):

    def __init__(self, recv_fn, send_fn):
        self.inbuffer = ""
        self.recv_fn = recv_fn
        self.send_fn = send_fn

    def recv_msg(self):
        while True:
            try:
                (self.inbuffer, ret) = unmarshall(self.inbuffer)
                return ret
            except NeedMoreData:
                newdata = self.recv_fn(1)
                if newdata != "":
                    # self.mutter("IN: %r" % newdata)
                    self.inbuffer += newdata

    def send_msg(self, data):
        marshalled_data = marshall(data)
        # self.mutter("OUT: %r" % marshalled_data)
        self.send_fn(marshalled_data)

    def send_success(self, *contents):
        self.send_msg([literal("success"), list(contents)])


SVN_PORT = 3690


def feed_editor(conn, editor):
    tokens = {}
    diff = {}
    txdelta_handler = {}
    # Process commands
    while True:
        command, args = conn.recv_msg()
        if command == "target-rev":
            editor.set_target_revision(args[0])
        elif command == "open-root":
            if len(args[0]) == 0:
                token = editor.open_root()
            else:
                token = editor.open_root(args[0][0])
            tokens[args[1]] = token
        elif command == "delete-entry":
            tokens[args[2]].delete_entry(args[0], args[1])
        elif command == "add-dir":
            if len(args[3]) == 0:
                token = tokens[args[1]].add_directory(args[0])
            else:
                token = tokens[args[1]].add_directory(
                    args[0], args[3][0], args[4][0])
            tokens[args[2]] = token
        elif command == "open-dir":
            tokens[args[2]] = tokens[args[1]].open_directory(args[0], args[3])
        elif command == "change-dir-prop":
            if len(args[2]) == 0:
                tokens[args[0]].change_prop(args[1], None)
            else:
                tokens[args[0]].change_prop(args[1], args[2][0])
        elif command == "close-dir":
            tokens[args[0]].close()
        elif command == "absent-dir":
            tokens[args[1]].absent(args[0])
        elif command == "add-file":
            if len(args[3]) == 0:
                token = tokens[args[1]].add_file(args[0])
            else:
                token = tokens[args[1]].add_file(
                    args[0], args[3][0], args[4][0])
            tokens[args[2]] = token
        elif command == "open-file":
            tokens[args[2]] = tokens[args[1]].open_file(args[0], args[3])
        elif command == "apply-textdelta":
            if len(args[1]) == 0:
                txdelta_handler[args[0]] = tokens[args[0]].apply_textdelta(
                    None)
            else:
                txdelta_handler[args[0]] = tokens[args[0]].apply_textdelta(
                    args[1][0])
            diff[args[0]] = ""
        elif command == "textdelta-chunk":
            diff[args[0]] += args[1]
        elif command == "textdelta-end":
            for w in unpack_svndiff0(diff[args[0]]):
                txdelta_handler[args[0]](w)
            txdelta_handler[args[0]](None)
        elif command == "change-file-prop":
            if len(args[2]) == 0:
                tokens[args[0]].change_prop(args[1], None)
            else:
                tokens[args[0]].change_prop(args[1], args[2][0])
        elif command == "close-file":
            if len(args[1]) == 0:
                tokens[args[0]].close()
            else:
                tokens[args[0]].close(args[1][0])
        elif command == "close-edit":
            editor.close()
            break
        elif command == "abort-edit":
            editor.abort()
            break

    conn.send_success()
    conn._unpack()


class Reporter(object):

    __slots__ = ('conn', 'editor')

    def __init__(self, conn, editor):
        self.conn = conn
        self.editor = editor

    def set_path(self, path, rev, start_empty=False, lock_token=None,
                 depth=None):
        args = [path, rev, start_empty]
        if lock_token is not None:
            args.append([lock_token])
        else:
            args.append([])
        if depth is not None:
            args.append(depth)

        self.conn.send_msg([literal("set-path"), args])

    def delete_path(self, path):
        self.conn.send_msg([literal("delete-path"), [path]])

    def link_path(self, path, url, rev, start_empty=False, lock_token=None,
                  depth=None):
        args = [path, url, rev, start_empty]
        if lock_token is not None:
            args.append([lock_token])
        else:
            args.append([])
        if depth is not None:
            args.append(depth)

        self.conn.send_msg([literal("link-path"), args])

    def finish(self):
        self.conn.send_msg([literal("finish-report"), []])
        self.conn.recv_msg()
        feed_editor(self.conn, self.editor)
        self.conn.busy = False

    def abort(self):
        self.conn.send_msg([literal("abort-report"), []])
        self.conn.busy = False


class Editor(object):

    __slots__ = ('conn')

    def __init__(self, conn):
        self.conn = conn

    def set_target_revision(self, revnum):
        self.conn.send_msg([literal("target-rev"), [revnum]])

    def open_root(self, base_revision=None):
        id = generate_random_id()
        if base_revision is None:
            baserev = []
        else:
            baserev = [base_revision]
        self.conn.send_msg([literal("open-root"), [baserev, id]])
        self.conn._open_ids = []
        return DirectoryEditor(self.conn, id)

    def close(self):
        self.conn.send_msg([literal("close-edit"), []])

    def abort(self):
        self.conn.send_msg([literal("abort-edit"), []])


class DirectoryEditor(object):

    __slots__ = ('conn', 'id')

    def __init__(self, conn, id):
        self.conn = conn
        self.id = id
        self.conn._open_ids.append(id)

    def add_file(self, path, copyfrom_path=None, copyfrom_rev=-1):
        self._is_last_open()
        child = generate_random_id()
        if copyfrom_path is not None:
            copyfrom_data = [copyfrom_path, copyfrom_rev]
        else:
            copyfrom_data = []
        self.conn.send_msg([literal("add-file"),
                           [path, self.id, child, copyfrom_data]])
        return FileEditor(self.conn, child)

    def open_file(self, path, base_revnum):
        self._is_last_open()
        child = generate_random_id()
        self.conn.send_msg([literal("open-file"),
                           [path, self.id, child, base_revnum]])
        return FileEditor(self.conn, child)

    def delete_entry(self, path, base_revnum):
        self._is_last_open()
        self.conn.send_msg([literal("delete-entry"),
                           [path, base_revnum, self.id]])

    def add_directory(self, path, copyfrom_path=None, copyfrom_rev=-1):
        self._is_last_open()
        child = generate_random_id()
        if copyfrom_path is not None:
            copyfrom_data = [copyfrom_path, copyfrom_rev]
        else:
            copyfrom_data = []
        self.conn.send_msg([literal("add-dir"),
                           [path, self.id, child, copyfrom_data]])
        return DirectoryEditor(self.conn, child)

    def open_directory(self, path, base_revnum):
        self._is_last_open()
        child = generate_random_id()
        self.conn.send_msg([literal("open-dir"),
                           [path, self.id, child, base_revnum]])
        return DirectoryEditor(self.conn, child)

    def change_prop(self, name, value):
        self._is_last_open()
        if value is None:
            value = []
        else:
            value = [value]
        self.conn.send_msg([literal("change-dir-prop"),
                           [self.id, name, value]])

    def _is_last_open(self):
        assert self.conn._open_ids[-1] == self.id

    def close(self):
        self._is_last_open()
        self.conn._open_ids.pop()
        self.conn.send_msg([literal("close-dir"), [self.id]])


class FileEditor(object):

    __slots__ = ('conn', 'id')

    def __init__(self, conn, id):
        self.conn = conn
        self.id = id
        self.conn._open_ids.append(id)

    def _is_last_open(self):
        assert self.conn._open_ids[-1] == self.id

    def close(self, checksum=None):
        self._is_last_open()
        self.conn._open_ids.pop()
        if checksum is None:
            checksum = []
        else:
            checksum = [checksum]
        self.conn.send_msg([literal("close-file"), [self.id, checksum]])

    def apply_textdelta(self, base_checksum=None):
        self._is_last_open()
        if base_checksum is None:
            base_check = []
        else:
            base_check = [base_checksum]
        self.conn.send_msg([literal("apply-textdelta"),
                           [self.id, base_check]])
        self.conn.send_msg([literal("textdelta-chunk"),
                           [self.id, SVNDIFF0_HEADER]])

        def send_textdelta(delta):
            if delta is None:
                self.conn.send_msg([literal("textdelta-end"), [self.id]])
            else:
                self.conn.send_msg([literal("textdelta-chunk"),
                                   [self.id, pack_svndiff0_window(delta)]])
        return send_textdelta

    def change_prop(self, name, value):
        self._is_last_open()
        if value is None:
            value = []
        else:
            value = [value]
        self.conn.send_msg([literal("change-file-prop"),
                           [self.id, name, value]])


def mark_busy(unbound):

    def convert(self, *args, **kwargs):
        self.busy = True
        try:
            ret = unbound(self, *args, **kwargs)
        finally:
            self.busy = False
        return ret

    convert.__doc__ = unbound.__doc__
    convert.__name__ = unbound.__name__
    return convert


def unmarshall_dirent(d):
    ret = {
        "name": d[0],
        "kind": d[1],
        "size": d[2],
        "has-props": bool(d[3]),
        "created-rev": d[4],
        }
    if d[5] != []:
        ret["created-date"] = d[5]
    if d[6] != []:
        ret["last-author"] = d[6]
    return ret


class SVNClient(SVNConnection):

    def __init__(self, url, progress_cb=None, auth=None, config=None,
                 client_string_func=None, open_tmp_file_func=None):
        self.url = url
        (type, opaque) = urlparse.splittype(url)
        assert type in ("svn", "svn+ssh")
        (host, path) = urlparse.splithost(opaque)
        self._progress_cb = progress_cb
        self._auth = auth
        self._config = config
        self._client_string_func = client_string_func
        # open_tmp_file_func is ignored, as it is not needed for svn://
        if type == "svn":
            (recv_func, send_func) = self._connect(host)
        else:
            (recv_func, send_func) = self._connect_ssh(host)
        super(SVNClient, self).__init__(recv_func, send_func)
        (min_version, max_version, _, self._server_capabilities) = (
            self._recv_greeting())
        self.send_msg(
            [max_version,
             [literal(x) for x in CAPABILITIES
                 if x in self._server_capabilities],
             self.url])
        (self._server_mechanisms, mech_arg) = self._unpack()
        if self._server_mechanisms != []:
            # FIXME: Support other mechanisms as well
            self.send_msg([literal("ANONYMOUS"),
                          [base64.b64encode(
                              "anonymous@%s" % socket.gethostname())]])
            self.recv_msg()
        msg = self._unpack()
        if len(msg) > 2:
            self._server_capabilities += msg[2]
        (self._uuid, self._root_url) = msg[0:2]
        self.busy = False

    def _unpack(self):
        msg = self.recv_msg()
        if msg[0] == "failure":
            if isinstance(msg[1], str):
                raise SubversionException(*msg[1])
            num = msg[1][0][0]
            msg = msg[1][0][1]
            if num == ERR_RA_SVN_UNKNOWN_CMD:
                raise NotImplementedError(msg)
            raise SubversionException(msg, num)
        assert msg[0] == "success", "Got: %r" % msg
        assert len(msg) == 2
        return msg[1]

    def _recv_greeting(self):
        greeting = self._unpack()
        assert len(greeting) == 4
        return greeting

    _recv_ack = _unpack

    def _connect(self, host):
        (host, port) = urlparse.splitnport(host, SVN_PORT)
        sockaddrs = socket.getaddrinfo(
               host, port, socket.AF_UNSPEC,
               socket.SOCK_STREAM, 0, 0)
        self._socket = None
        err = RuntimeError('no addresses for %s:%s' % (host, port))
        for (family, socktype, proto, canonname, sockaddr) in sockaddrs:
            try:
                self._socket = socket.socket(family, socktype, proto)
                self._socket.connect(sockaddr)
            except socket.error as err:
                if self._socket is not None:
                    self._socket.close()
                self._socket = None
                continue
            break
        if self._socket is None:
            raise err
        self._socket.setblocking(True)
        return (self._socket.recv, self._socket.send)

    def _connect_ssh(self, host):
        (user, host) = urlparse.splituser(host)
        if user is not None:
            (user, password) = urlparse.splitpassword(user)
        else:
            password = None
        (host, port) = urlparse.splitnport(host, 22)
        self._tunnel = get_ssh_vendor().connect_ssh(
            user, password, host, port, ["svnserve", "-t"])
        return (self._tunnel.recv, self._tunnel.send)

    def get_file_revs(self, path, start, end, file_rev_handler):
        raise NotImplementedError(self.get_file_revs)

    @mark_busy
    def get_locations(self, path, peg_revision, location_revisions):
        self.send_msg([literal("get-locations"), [path, peg_revision,
                      location_revisions]])
        self._recv_ack()
        ret = {}
        while True:
            msg = self.recv_msg()
            if msg == "done":
                break
            ret[msg[0]] = msg[1]
        self._unparse()
        return ret

    def get_locks(self, path):
        self.send_msg([literal("get-lock"), [path]])
        self._recv_ack()
        return self._unpack()

    def lock(self, path_revs, comment, steal_lock, lock_func):
        raise NotImplementedError(self.lock)

    def unlock(self, path_tokens, break_lock, lock_func):
        raise NotImplementedError(self.unlock)

    def mergeinfo(self, paths, revision=-1, inherit=None,
                  include_descendants=False):
        raise NotImplementedError(self.mergeinfo)

    def location_segments(self, path, start_revision, end_revision,
                          include_merged_revisions=False):
        args = [path]
        if start_revision is None or start_revision == -1:
            args.append([])
        else:
            args.append([start_revision])
        if end_revision is None or end_revision == -1:
            args.append([])
        else:
            args.append([end_revision])
        args.append(include_merged_revisions)
        self.send_msg([literal("get-location-segments"), args])
        self._recv_ack()
        while True:
            msg = self.recv_msg()
            if msg == "done":
                break
            yield msg
        self._unpack()

    def get_location_segments(self, path, start_revision, end_revision, rcvr):
        for msg in self.location_segments(path, start_revision, end_revision):
            rcvr(*msg)

    def has_capability(self, capability):
        return capability in self._server_capabilities

    @mark_busy
    def check_path(self, path, revision=None):
        args = [path]
        if revision is None or revision == -1:
            args.append([])
        else:
            args.append([revision])
        self.send_msg([literal("check-path"), args])
        self._recv_ack()
        ret = self._unpack()[0]
        return {"dir": NODE_DIR, "file": NODE_FILE, "unknown": NODE_UNKNOWN,
                "none": NODE_NONE}[ret]

    def get_lock(self, path):
        self.send_msg([literal("get-lock"), [path]])
        self._recv_ack()
        ret = self._unpack()
        if len(ret) == 0:
            return None
        else:
            return ret[0]

    @mark_busy
    def get_dir(self, path, revision=-1, dirent_fields=0, want_props=True,
                want_contents=True):
        args = [path]
        if revision is None or revision == -1:
            args.append([])
        else:
            args.append([revision])

        args += [want_props, want_contents]

        fields = []
        if dirent_fields & DIRENT_KIND:
            fields.append(literal("kind"))
        if dirent_fields & DIRENT_SIZE:
            fields.append(literal("size"))
        if dirent_fields & DIRENT_HAS_PROPS:
            fields.append(literal("has-props"))
        if dirent_fields & DIRENT_CREATED_REV:
            fields.append(literal("created-rev"))
        if dirent_fields & DIRENT_TIME:
            fields.append(literal("time"))
        if dirent_fields & DIRENT_LAST_AUTHOR:
            fields.append(literal("last-author"))
        args.append(fields)

        self.send_msg([literal("get-dir"), args])
        self._recv_ack()
        ret = self._unpack()
        fetch_rev = ret[0]
        props = dict(ret[1])
        dirents = {}
        for d in ret[2]:
            entry = unmarshall_dirent(d)
            dirents[entry["name"]] = entry

        return (dirents, fetch_rev, props)

    @mark_busy
    def stat(self, path, revision=-1):
        args = [path]
        if revision is None or revision == -1:
            args.append([revision])
        else:
            args.append([])

        self.send_msg([literal("stat"), args])
        self._recv_ack()
        ret = self._unpack()
        if len(ret) == 0:
            return None
        return unmarshall_dirent(ret[0])

    @mark_busy
    def get_file(self, path, stream, revision=-1):
        raise NotImplementedError(self.get_file)

    def change_rev_prop(self, rev, name, value):
        args = [rev, name]
        if value is not None:
            args.append(value)
        self.send_msg([literal("change-rev-prop"), args])
        self._recv_ack()
        self._unparse()

    def get_commit_editor(self, revprops, callback=None, lock_tokens=None,
                          keep_locks=False):
        args = [revprops[properties.PROP_REVISION_LOG]]
        if lock_tokens is not None:
            args.append(list(lock_tokens.items()))
        else:
            args.append([])
        args.append(keep_locks)
        if len(revprops) > 1:
            args.append(list(revprops.items()))
        self.send_msg([literal("commit"), args])
        self._recv_ack()
        raise NotImplementedError(self.get_commit_editor)

    def rev_proplist(self, revision):
        self.send_msg([literal("rev-proplist"), [revision]])
        self._recv_ack()
        return dict(self._unpack()[0])

    def rev_prop(self, revision, name):
        self.send_msg([literal("rev-prop"), [revision, name]])
        self._recv_ack()
        ret = self._unpack()
        if len(ret) == 0:
            return None
        else:
            return ret[0]

    @mark_busy
    def replay(self, revision, low_water_mark, update_editor,
               send_deltas=True):
        self.send_msg([literal("replay"), [revision, low_water_mark,
                      send_deltas]])
        self._recv_ack()
        feed_editor(self, update_editor)
        self._unpack()

    @mark_busy
    def replay_range(self, start_revision, end_revision, low_water_mark, cbs,
                     send_deltas=True):
        self.send_msg([literal("replay-range"), [start_revision, end_revision,
                      low_water_mark, send_deltas]])
        self._recv_ack()
        for i in range(start_revision, end_revision+1):
            msg = self.recv_msg()
            assert msg[0] == "revprops"
            edit = cbs[0](i, dict(msg[1]))
            feed_editor(self, edit)
            cbs[1](i, dict(msg[1]), edit)
        self._unpack()

    def do_switch(self, revision_to_update_to, update_target, recurse,
                  switch_url, update_editor, depth=None):
        args = []
        if revision_to_update_to is None or revision_to_update_to == -1:
            args.append([])
        else:
            args.append([revision_to_update_to])
        args.append(update_target)
        args.append(recurse)
        args.append(switch_url)
        if depth is not None:
            args.append(literal(depth))

        self.busy = True
        try:
            self.send_msg([literal("switch"), args])
            self._recv_ack()
            return Reporter(self, update_editor)
        except:
            self.busy = False
            raise

    def do_update(self, revision_to_update_to, update_target, recurse,
                  update_editor, depth=None):
        args = []
        if revision_to_update_to is None or revision_to_update_to == -1:
            args.append([])
        else:
            args.append([revision_to_update_to])
        args.append(update_target)
        args.append(recurse)
        if depth is not None:
            args.append(literal(depth))

        self.busy = True
        try:
            self.send_msg([literal("update"), args])
            self._recv_ack()
            return Reporter(self, update_editor)
        except:
            self.busy = False
            raise

    def do_diff(self, revision_to_update, diff_target, versus_url, diff_editor,
                recurse=True, ignore_ancestry=False, text_deltas=False,
                depth=None):
        args = []
        if revision_to_update is None or revision_to_update == -1:
            args.append([])
        else:
            args.append([revision_to_update])
        args += [diff_target, recurse, ignore_ancestry, versus_url,
                 text_deltas]
        if depth is not None:
            args.append(literal(depth))
        self.busy = True
        try:
            self.send_msg([literal("diff"), args])
            self._recv_ack()
            return Reporter(self, diff_editor)
        except:
            self.busy = False
            raise

    def get_repos_root(self):
        return self._root_url

    @mark_busy
    def get_latest_revnum(self):
        self.send_msg([literal("get-latest-rev"), []])
        self._recv_ack()
        return self._unpack()[0]

    @mark_busy
    def get_dated_rev(self, date):
        self.send_msg([literal("get-dated-rev"), [date]])
        self._recv_ack()
        return self._unpack()[0]

    @mark_busy
    def reparent(self, url):
        self.send_msg([literal("reparent"), [url]])
        self._recv_ack()
        self._unpack()
        self.url = url

    def get_uuid(self):
        return self._uuid

    @mark_busy
    def log(self, paths, start, end, limit=0, discover_changed_paths=True,
            strict_node_history=True, include_merged_revisions=True,
            revprops=None):
        args = [paths]
        if start is None or start == -1:
            args.append([])
        else:
            args.append([start])
        if end is None or end == -1:
            args.append([])
        else:
            args.append([end])
        args.append(discover_changed_paths)
        args.append(strict_node_history)
        args.append(limit)
        args.append(include_merged_revisions)
        if revprops is None:
            args.append(literal("all-revprops"))
            args.append([])
        else:
            args.append(literal("revprops"))
            args.append(revprops)

        self.send_msg([literal("log"), args])
        self._recv_ack()
        while True:
            msg = self.recv_msg()
            if msg == "done":
                break
            paths = {}
            for p, action, cfd in msg[0]:
                if len(cfd) == 0:
                    paths[p] = (str(action), None, -1)
                else:
                    paths[p] = (str(action), cfd[0], cfd[1])

            if len(msg) > 5:
                has_children = msg[5]
            else:
                has_children = None
            if len(msg) > 6 and msg[6]:
                revno = None
            else:
                revno = msg[1]  # noqa: F841
                # TODO(jelmer): Do something with revno
            revprops = {}
            if len(msg[2]) != 0:
                revprops[properties.PROP_REVISION_AUTHOR] = msg[2][0]
            if len(msg[3]) != 0:
                revprops[properties.PROP_REVISION_DATE] = msg[3][0]
            if len(msg[4]) != 0:
                revprops[properties.PROP_REVISION_LOG] = msg[4][0]
            if len(msg) > 8:
                revprops.update(dict(msg[8]))
            yield paths, msg[1], revprops, has_children

        self._unpack()

    def get_log(self, callback, *args, **kwargs):
        for (paths, rev, props, has_children) in self.log(*args, **kwargs):
            if has_children is None:
                callback(paths, rev, props)
            else:
                callback(paths, rev, props, has_children)


MIN_VERSION = 2
MAX_VERSION = 2
CAPABILITIES = ["edit-pipeline", "bazaar", "log-revprops"]
MECHANISMS = ["ANONYMOUS"]


class SVNServer(SVNConnection):

    def __init__(self, backend, recv_fn, send_fn, logf=None):
        self.backend = backend
        self._stop = False
        self._logf = logf
        super(SVNServer, self).__init__(recv_fn, send_fn)

        self.send_success(
            MIN_VERSION, MAX_VERSION, [literal(x) for x in MECHANISMS],
            [literal(x) for x in CAPABILITIES])

    def send_mechs(self):
        self.send_success([literal(x) for x in MECHANISMS], "")

    def send_failure(self, *contents):
        self.send_msg([literal("failure"), list(contents)])

    def send_ack(self):
        self.send_success([], "")

    def send_unknown(self, cmd):
        self.send_failure(
            [ERR_RA_SVN_UNKNOWN_CMD,
             "Unknown command '%s'" % cmd, __file__, 52])

    def get_latest_rev(self):
        self.send_ack()
        self.send_success(self.repo_backend.get_latest_revnum())

    def check_path(self, path, rev):
        if len(rev) == 0:
            revnum = None
        else:
            revnum = rev[0]
        kind = self.repo_backend.check_path(path, revnum)
        self.send_ack()
        self.send_success(literal({
            NODE_NONE: "none",
            NODE_DIR: "dir",
            NODE_FILE: "file",
            NODE_UNKNOWN: "unknown"}[kind]))

    def log(self, target_path, start_rev, end_rev, changed_paths,
            strict_node, limit=None, include_merged_revisions=False,
            all_revprops=None, revprops=None):
        def send_revision(revno, author, date, message, changed_paths=None):
            changes = []
            if changed_paths is not None:
                for p, (action, cf, cr) in changed_paths.items():
                    if cf is not None:
                        changes.append((p, literal(action), (cf, cr)))
                    else:
                        changes.append((p, literal(action), ()))
            self.send_msg([changes, revno, [author], [date], [message]])
        self.send_ack()
        if len(start_rev) == 0:
            start_revnum = None
        else:
            start_revnum = start_rev[0]
        if len(end_rev) == 0:
            end_revnum = None
        else:
            end_revnum = end_rev[0]
        self.repo_backend.log(send_revision, target_path, start_revnum,
                              end_revnum, changed_paths, strict_node, limit)
        self.send_msg(literal("done"))
        self.send_success()

    def open_backend(self, url):
        (rooturl, location) = urlparse.splithost(url)
        self.repo_backend, self.relpath = self.backend.open_repository(
             location)

    def reparent(self, parent):
        self.open_backend(parent)
        self.send_ack()
        self.send_success()

    def stat(self, path, rev):
        if len(rev) == 0:
            revnum = None
        else:
            revnum = rev[0]
        self.send_ack()
        dirent = self.repo_backend.stat(path, revnum)
        if dirent is None:
            self.send_success([])
        else:
            args = [dirent["name"], dirent["kind"], dirent["size"],
                    dirent["has-props"], dirent["created-rev"]]
            if "created-date" in dirent:
                args.append([dirent["created-date"]])
            else:
                args.append([])
            if "last-author" in dirent:
                args.append([dirent["last-author"]])
            else:
                args.append([])
            self.send_success([args])

    def commit(self, logmsg, locks, keep_locks=False, rev_props=None):
        self.send_failure([ERR_UNSUPPORTED_FEATURE,
                          "commit not yet supported", __file__, 42])

    def rev_proplist(self, revnum):
        self.send_ack()
        revprops = self.repo_backend.rev_proplist(revnum)
        self.send_success(list(revprops.items()))

    def rev_prop(self, revnum, name):
        self.send_ack()
        revprops = self.repo_backend.rev_proplist(revnum)
        if name in revprops:
            self.send_success([revprops[name]])
        else:
            self.send_success()

    def get_locations(self, path, peg_revnum, revnums):
        self.send_ack()
        locations = self.repo_backend.get_locations(path, peg_revnum, revnums)
        for rev, path in locations.items():
            self.send_msg([rev, path])
        self.send_msg(literal("done"))
        self.send_success()

    def update(self, rev, target, recurse, depth=None,
               send_copyfrom_param=True):
        self.send_ack()
        while True:
            msg = self.recv_msg()
            assert msg[0] in ["set-path", "finish-report"]
            if msg[0] == "finish-report":
                break

        self.send_ack()

        if len(rev) == 0:
            revnum = None
        else:
            revnum = rev[0]
        self.repo_backend.update(Editor(self), revnum, target, recurse)
        self.send_success()
        client_result = self.recv_msg()
        if client_result[0] == "success":
            return
        else:
            self.mutter("Client reported error during update: %r" %
                        client_result)
            # Needs to be sent back to the client to display
            self.send_failure(client_result[1][0])

    commands = {
            "get-latest-rev": get_latest_rev,
            "log": log,
            "update": update,
            "check-path": check_path,
            "reparent": reparent,
            "stat": stat,
            "commit": commit,
            "rev-proplist": rev_proplist,
            "rev-prop": rev_prop,
            "get-locations": get_locations,
            # FIXME: get-dated-rev
            # FIXME: get-file
            # FIXME: get-dir
            # FIXME: check-path
            # FIXME: switch
            # FIXME: status
            # FIXME: diff
            # FIXME: get-file-revs
            # FIXME: replay
    }

    def send_auth_request(self):
        pass

    def serve(self):
        self.send_greeting()
        msg = self.recv_msg()
        version = msg[0]
        capabilities = msg[1]
        url = msg[2]
        if len(msg) > 3:
            self.client_user_agent = msg[3]
        else:
            self.client_user_agent = None
        self.capabilities = capabilities
        self.version = version
        self.url = url
        self.mutter("client supports:")
        self.mutter("  version %r" % version)
        self.mutter("  capabilities %r " % capabilities)
        self.send_mechs()

        (mech, args) = self.recv_msg()
        # TODO: Proper authentication
        self.send_success()

        self.open_backend(url)
        self.send_success(self.repo_backend.get_uuid(), url)

        # Expect:
        while not self._stop:
            (cmd, args) = self.recv_msg()
            if cmd not in self.commands:
                self.mutter("client used unknown command %r" % cmd)
                self.send_unknown(cmd)
                return
            else:
                self.commands[cmd](self, *args)

    def close(self):
        self._stop = True

    def mutter(self, text):
        if self._logf is not None:
            self._logf.write("%s\n" % text)


class TCPSVNRequestHandler(StreamRequestHandler):

    def __init__(self, request, client_address, server):
        self._server = server
        StreamRequestHandler.__init__(
            self, request, client_address, server)

    def handle(self):
        server = SVNServer(
            self._server._backend, self.rfile.read,
            self.wfile.write, self._server._logf)
        try:
            server.serve()
        except socket.error as e:
            if e.args[0] == EPIPE:
                return
            raise


class TCPSVNServer(TCPServer):

    allow_reuse_address = True
    serve = TCPServer.serve_forever

    def __init__(self, backend, addr, logf=None):
        self._logf = logf
        self._backend = backend
        TCPServer.__init__(self, addr, TCPSVNRequestHandler)
