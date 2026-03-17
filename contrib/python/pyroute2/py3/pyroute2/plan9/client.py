import json
import os
import pwd
import struct

from pyroute2.common import AddrPool
from pyroute2.netlink.core import (
    AsyncCoreSocket,
    CoreMessageQueue,
    CoreStreamProtocol,
)
from pyroute2.netlink.coredata import CoreConfig, CoreSocketSpec
from pyroute2.plan9 import (
    Marshal9P,
    msg_tattach,
    msg_tcall,
    msg_tread,
    msg_tversion,
    msg_twalk,
    msg_twrite,
)


class Plan9ClientSocket(AsyncCoreSocket):
    '''9p2000 client.

    * address -- `('address', port)` to listen on
    * use_socket -- alternatively, provide a connected SOCK_STRAM socket
    '''

    def __init__(self, address=None, use_socket=None, use_event_loop=None):
        self.spec = CoreSocketSpec(
            CoreConfig(
                tag_field='tag',
                target='localhost',
                netns=None,
                address=address,
                use_socket=use_socket is not None,
                use_event_loop=use_event_loop is not None,
            )
        )
        self.marshal = Marshal9P()
        self.wnames = {'': 0}
        self.cwd = 0
        self.fid_pool = AddrPool(minaddr=0x00000001, maxaddr=0x0000FFFF)
        super().__init__(use_socket=use_socket, use_event_loop=use_event_loop)

    def enqueue(self, data, addr):
        tag = struct.unpack_from('H', data, 5)[0]
        return self.msg_queue.put_nowait(tag, data)

    async def setup_endpoint(self, loop=None):
        if getattr(self.local, 'transport', None) is not None:
            return
        self.local.msg_queue = CoreMessageQueue(event_loop=self.event_loop)
        if self.status['use_socket']:
            address = {'sock': self.use_socket}
        else:
            address = {
                'host': self.status['address'][0],
                'port': self.status['address'][1],
            }
        self.local.transport, self.local.protocol = (
            await self.event_loop.create_connection(
                lambda: CoreStreamProtocol(
                    self.connection_lost,
                    self.enqueue,
                    self._error_event,
                    self.status,
                ),
                **address,
            )
        )

    async def start_session(self):
        '''Initiate 9p2000 session.

        One must await this routine before running any other requests.
        '''
        await self.setup_endpoint()
        await self.version()
        await self.auth()
        await self.attach()

    async def request(self, msg, tag=0):
        await self.setup_endpoint()
        if tag == 0:
            tag = self.addr_pool.alloc()
        try:
            msg['header']['tag'] = tag
            msg.reset()
            msg.encode()
            self.msg_queue.ensure_tag(tag)
            self.transport.write(msg.data)
            return [x async for x in self.get(msg_seq=tag)][0]
        finally:
            self.addr_pool.free(tag, ban=0xFF)

    async def version(self):
        '''`Tverion` request. No arguments required.'''
        m = msg_tversion()
        m['header']['tag'] = 0xFFFF
        m['msize'] = 8192
        m['version'] = '9P2000'
        return await self.request(m, tag=0xFFFF)

    async def auth(self):
        pass

    async def attach(self, aname=''):
        '''`Tattach` request.

        * `aname` (optional) -- aname to attach to
        '''
        m = msg_tattach()
        m['fid'] = 0
        m['afid'] = 0xFFFFFFFF
        m['uname'] = pwd.getpwuid(os.getuid()).pw_name
        m['aname'] = aname
        return await self.request(m)

    async def walk(self, path, newfid=None, fid=None):
        '''`Twalk` request.

        * `path` -- string path to the file
        * `newfid` (optional) -- use this fid to store the info
        * `fid` (optional) -- use this fid to walk from, otherwise walk
          from the current directory for this client session
        '''
        m = msg_twalk()
        m['fid'] = self.cwd if fid is None else fid
        m['newfid'] = newfid if newfid is not None else self.fid_pool.alloc()
        m['wname'] = path.split(os.path.sep)
        self.wnames[path] = m['newfid']
        return await self.request(m)

    async def fid(self, path):
        '''Walk the path and return `fid` to the required file.

        * `path` -- string path to the file
        '''
        if path not in self.wnames:
            newfid = self.fid_pool.alloc()
            await self.walk(path, newfid)
            self.wnames[path] = newfid
        return self.wnames[path]

    async def read(self, fid, offset=0, count=8192):
        '''`Tread` request.

        * `fid` -- fid of the file to read from
        * `offset` (optional, default 0) -- read offset
        * `count` (optional, default 8192) -- read count
        '''
        m = msg_tread()
        m['fid'] = fid
        m['offset'] = offset
        m['count'] = count
        return await self.request(m)

    async def write(self, fid, data, offset=0):
        '''`Twrite` request.

        * `fid` -- fid of the file to write to
        * `data` -- bytes to write
        * `offset` (optional, default 0) -- write offset
        '''
        m = msg_twrite()
        m['fid'] = fid
        m['offset'] = 0
        m['data'] = data
        return await self.request(m)

    async def call(
        self,
        fid,
        argv=None,
        kwarg=None,
        data=b'',
        data_arg='data',
        loader=json.loads,
    ):
        '''`Tcall` request.

        * `fid` -- fid of the file that represents a registered function
        * `argv` (optional) -- positional arguments as an iterable
        * `kwarg` (optional) -- keyword arguments as a dictionary
        * `data` (optional) -- optional binary data
        * `data_arg` (optional) -- name of the argument to use with
          the binary data
        * `loader` (optional, default `json.loads`) -- loader for the
          response data
        '''
        spec = {
            'argv': argv if argv is not None else [],
            'kwarg': kwarg if kwarg is not None else {},
            'data_arg': data_arg,
        }
        m = msg_tcall()
        m['fid'] = fid
        m['text'] = json.dumps(spec)
        m['data'] = data
        response = await self.request(m)
        return loader(response['data'])
