import grp
import io
import json
import os
import pwd
import time
from dataclasses import dataclass
from functools import partial

from pyroute2.plan9 import Plan9Exit, Qid, Stat, Tcall, Tread, Twrite


def _publish_function_w(session, inode, request, response):
    inode.metadata.dirty = True
    inode.data.seek(0)
    inode.data.truncate()
    inode.data.write(request['data'])
    response['count'] = len(request['data'])
    return response


def _publish_function_r(
    func, loader, dumper, session, inode, request, response
):
    if request['offset'] == 0 and inode.metadata.has_new_data():
        try:
            kwarg = loader(inode.data.getvalue())
            ret = func(**kwarg)
            inode.metadata.dirty = False
        except Plan9Exit:
            raise
        except Exception as e:
            ret = e
        inode.data.seek(0)
        inode.data.truncate()
        inode.data.write(dumper(ret))

    inode.data.seek(request['offset'])
    response['data'] = inode.data.read(request['count'])
    return response


def _publish_function_c(
    func, loader, dumper, session, inode, request, response
):
    spec = loader(request['text'])

    if 'argv' not in spec:
        spec['argv'] = []
    if 'kwarg' not in spec:
        spec['kwarg'] = {}
    if 'data_arg' not in spec:
        spec['data_arg'] = 'data'
    data = request['data']
    if data:
        spec['kwarg'][spec['data_arg']] = data
    ret = func(*spec['argv'], **spec['kwarg'])

    response['err'] = 0
    response['text'] = ''
    response['data'] = dumper(ret)

    return response


@dataclass
class InodeMetadata:
    call_on_read: bool = False
    dirty: bool = False

    def has_new_data(self) -> bool:
        return self.dirty or self.call_on_read


class Inode:
    children = None
    parents = None
    qid = None
    stat = None
    data = None
    callbacks = None

    def __init__(
        self,
        name,
        path,
        data='',
        qtype=0,
        mode=0o600,
        uid=None,
        gid=None,
        parents=None,
        children=None,
    ):
        self.data = io.BytesIO(data.encode('utf-8'))
        self.parents = parents if parents is not None else set()
        self.children = children if children is not None else set()
        self.callbacks = {}
        self.metadata = InodeMetadata()
        self.stat = Stat()
        self.qid = Qid(qtype, 0, path)
        self.stat['uid'] = (
            uid if uid is not None else pwd.getpwuid(os.getuid()).pw_name
        )
        self.stat['gid'] = (
            gid if gid is not None else grp.getgrgid(os.getgid()).gr_name
        )
        self.stat['muid'] = self.stat['uid']
        self.stat['qid.type'] = self.qid['type']
        self.stat['qid.vers'] = self.qid['vers']
        self.stat['qid.path'] = self.qid['path']
        # shift qid.type 3 bytes left and OR with mode
        self.stat['mode'] = (self.qid['type'] << (8 * 3)) | mode
        self.stat['type'] = self.qid['path']
        self.stat['dev'] = 0
        self.stat['mtime'] = int(time.time())
        self.stat['atime'] = int(time.time())
        self.stat['name'] = name
        self.sync()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def sync(self):
        self.stat['length'] = len(self.data.getvalue())

    def get_parent(self, name=None):
        return tuple(self.parents)[0]

    def get_child(self, name):
        for child in self.children:
            if child.stat['name'] == name:
                return child
        raise KeyError('file not found')

    def register_function(
        self,
        func,
        loader=json.loads,
        dumper=lambda x: x if isinstance(x, bytes) else str(x).encode('utf-8'),
    ):
        self.add_callback(Twrite, _publish_function_w)
        self.add_callback(
            Tread, partial(_publish_function_r, func, loader, dumper)
        )
        self.add_callback(
            Tcall, partial(_publish_function_c, func, loader, dumper)
        )

    def add_callback(self, call, f):
        self.callbacks[call] = f
        return self

    def add_parent(self, inode):
        return self.parents.add(inode)

    def add_child(self, inode):
        inode.add_parent(self)
        return self.children.add(inode)

    def del_parent(self, inode, force=False):
        if force or len(self.parents) > 1:
            return self.parents.remove(inode)
        raise KeyError('can not remove the only parent')

    def del_child(self, inode):
        inode.del_parent(self, force=True)
        return self.children.remove(inode)

    def __id__(self):
        return self.qid['path']


class Filesystem:

    inodes = {}

    def __init__(self):
        self.__path = 255
        # create the root inode
        path = 0
        self.inodes[path] = root = Inode('/', path, qtype=0x80, mode=0o755)
        root.add_parent(root)

    @property
    def new_path(self):
        self.__path += 1
        return self.__path

    def walk(self, wname):
        inode = self.inodes[0]
        for name in wname.split(os.path.sep):
            if name == '':
                continue
            inode = inode.get_child(name)
        return inode

    def create(self, wname, qtype=0, mode=None, data=''):
        if mode is None:
            mode = 0o750 if qtype & 0x80 else 0o640
        pname, name = os.path.split(wname)
        parent = self.walk(pname)
        path = self.new_path
        inode = Inode(name, path, qtype=qtype, mode=mode, data=data)
        parent.add_child(inode)
        return inode

    def get_inode(self, path):
        return self.inodes[path]

    def set_inode(self, path, inode):
        self.inodes[path] = inode


class Session:

    fid_table = {}

    def __init__(self, filesystem):
        self.filesystem = filesystem

    @property
    def root(self):
        return self.filesystem.inodes[0]

    def set_fid(self, fid, inode):
        self.fid_table[fid] = inode

    def get_fid(self, fid):
        return self.fid_table[fid]

    def create(self, name, mode):
        pass
