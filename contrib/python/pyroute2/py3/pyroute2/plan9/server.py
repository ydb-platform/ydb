import asyncio
import json

from pyroute2.netlink.core import AsyncCoreSocket, CoreMessageQueue
from pyroute2.netlink.coredata import CoreConfig, CoreSocketSpec
from pyroute2.plan9 import (
    Marshal9P,
    Plan9Exit,
    Stat,
    Tattach,
    Tauth,
    Tcall,
    Tclunk,
    Tcreate,
    Topen,
    Tread,
    Tremove,
    Tstat,
    Tversion,
    Twalk,
    Twrite,
    Twstat,
    msg_rattach,
    msg_rcall,
    msg_rclunk,
    msg_rerror,
    msg_ropen,
    msg_rread,
    msg_rstat,
    msg_rversion,
    msg_rwalk,
    msg_rwrite,
    msg_rwstat,
)
from pyroute2.plan9.filesystem import Filesystem, Session

data = str(dir())


def get_exception_args(exc):
    args = []
    if hasattr(exc, 'errno'):
        args.append(exc.errno)
        args.append(exc.strerror)
    return args


def route(rtable, request, state):
    def decorator(f):
        rtable[request] = f
        return f

    return decorator


class Plan9ServerProtocol(asyncio.Protocol):
    rtable = {}

    def __init__(self, on_con_lost, marshal, filesystem):
        self.transport = None
        self.session = None
        self.filesystem = filesystem
        self.marshal = marshal
        self.on_con_lost = on_con_lost

    @route(rtable, request=Tversion, state=(None,))
    def t_version(self, req):
        m = msg_rversion()
        m['header']['tag'] = 0xFFFF
        m['msize'] = req['msize']
        m['version'] = '9P2000'
        return m

    @route(rtable, request=Tauth, state=(Tversion,))
    def t_auth(self, req):
        m = msg_rerror()
        m['ename'] = 'no authentication required'
        return m

    @route(rtable, request=Tattach, state=(Tauth,))
    def t_attach(self, req):
        m = msg_rattach()
        root = self.session.filesystem.inodes[0]
        self.session.set_fid(req['fid'], root)
        m['qid'] = root.qid
        return m

    @route(rtable, request=Twalk, state=(Tattach,))
    def t_walk(self, req):
        m = msg_rwalk()
        inode = self.session.get_fid(req['fid'])
        wqid = []
        if len(req['wname']) == 0:
            self.session.set_fid(req['newfid'], inode)
        else:
            for name in req['wname']:
                if name == '.':
                    continue
                elif name == '..':
                    inode = inode.get_parent()
                else:
                    inode = inode.get_child(name)
                wqid.append(inode.qid)
        m['wqid'] = wqid
        self.session.set_fid(req['newfid'], inode)
        return m

    @route(rtable, request=Tstat, state=(Twalk,))
    def t_stat(self, req):
        m = msg_rstat()
        inode = self.session.get_fid(req['fid'])
        inode.sync()
        m['stat'] = inode.stat
        return m

    @route(rtable, request=Twstat, state=(Twalk,))
    def t_wstat(self, req):
        m = msg_rwstat()
        return m

    @route(rtable, request=Topen, state=(Twalk, Tstat))
    def t_open(self, req):
        m = msg_ropen()
        m['qid'] = self.session.get_fid(req['fid']).qid
        m['iounit'] = 8192
        return m

    @route(rtable, request=Tcall, state=(Twalk, Topen, Tstat))
    def t_call(self, req):
        m = msg_rcall()
        inode = self.session.get_fid(req['fid'])
        m['err'] = 255
        if Tcall in inode.callbacks:
            m = inode.callbacks[Tcall](self.session, inode, req, m)
        return m

    @route(rtable, request=Twrite, state=(Topen,))
    def t_write(self, req):
        m = msg_rwrite()
        inode = self.session.get_fid(req['fid'])
        if Twrite in inode.callbacks:
            return inode.callbacks[Twrite](self.session, inode, req, m)
        if inode.qid['type'] & 0x80:
            raise TypeError('can not call write() on dir')
        inode.data.seek(req['offset'])
        m['count'] = inode.data.write(req['data'])
        return m

    @route(rtable, request=Tread, state=(Topen,))
    def t_read(self, req):
        m = msg_rread()
        inode = self.session.get_fid(req['fid'])
        if Tread in inode.callbacks:
            return inode.callbacks[Tread](self.session, inode, req, m)
        if inode.qid['type'] & 0x80:
            data = bytearray()
            offset = 0
            for child in inode.children:
                offset = Stat.encode_into(data, offset, child.stat)
            data = data[req['offset'] : req['offset'] + req['count']]
        else:
            inode.data.seek(req['offset'])
            data = inode.data.read(req['count'])
        m['data'] = data
        return m

    @route(rtable, request=Tclunk, state=(Topen, Tstat, Twalk, Tread))
    def t_clunk(self, req):
        return msg_rclunk()

    @route(rtable, request=Tcreate, state=(Twalk,))
    def t_create(self, req):
        return self.permission_denied(req)

    @route(rtable, request=Tremove, state=(Twalk,))
    def t_remove(self, req):
        return self.permission_denied(req)

    def permission_denied(self, req):
        r_message = msg_rerror()
        r_message['ename'] = 'permission denied'
        r_message['header']['tag'] = req['header']['tag']
        return r_message

    def error(self, e, tag=0):
        r_message = msg_rerror()
        spec = {
            'class': e.__class__.__name__,
            'argv': get_exception_args(e),
            'str': str(e),
        }
        r_message['ename'] = json.dumps(spec)
        r_message['header']['tag'] = tag
        r_message.encode()
        self.transport.write(r_message.data)

    def data_received(self, data):
        for t_message in self.marshal.parse(data):
            tag = t_message['header']['tag']
            try:
                r_message = self.rtable[t_message['header']['type']](
                    self, t_message
                )
                r_message['header']['tag'] = tag
                r_message.encode()
            except Plan9Exit as e:
                self.error(e, tag)
                self.transport.abort()
                self.transport.close()
                return
            except Exception as e:
                self.error(e, tag)
                return
            self.transport.write(r_message.data)

    def connection_made(self, transport):
        self.transport = transport
        self.session = Session(self.filesystem)


class Plan9ServerSocket(AsyncCoreSocket):
    '''9p2000 server.

    Requires either an IP address to listen on, or an open
    `SOCK_STREAM` socket to operate. An IP example, suitable
    to establish IPC between processes in one network:

    .. testcode::

        from pyroute2 import Plan9ClientSocket, Plan9ServerSocket

        address = ('localhost', 8149)
        p9server = Plan9ServerSocket(address=address)
        p9client = Plan9ClientSocket(address=address)

    Server/client running on a `socketpair()` suitable
    for internal API within one process, or between
    parent/child processes:

    .. testcode::

        from socket import socketpair

        from pyroute2 import Plan9ClientSocket, Plan9ServerSocket

        server, client = socketpair()
        p9server = Plan9ServerSocket(use_socket=server)
        p9client = Plan9ClientSocket(use_socket=client)


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
        self.filesystem = Filesystem()
        self.marshal = Marshal9P()
        super().__init__(use_socket=use_socket, use_event_loop=use_event_loop)

    def register_function(
        self,
        func,
        inode,
        loader=json.loads,
        dumper=lambda x: json.dumps(x).encode('utf-8'),
    ):
        '''Register a function to an file.

        The file usage:

        * `write()`: write arguments for the call as a json dictionary of
          keyword arguments to the file data.
        * `read()`:
            1. if the arguments were written to the data, call the function
               and write the result to the file data
            2. read the file data and return to the client
        * `call()`: protocol extension, `Tcall` = 80, `Rcall` = 81, make
          this in one turn.

        .. testcode::
            :hide:

            from pyroute2.plan9 import Tcall, Rcall

            assert Tcall == 80
            assert Rcall == 81

        Registering a function:

        .. testcode::

            def communicate(a, b):
                return a + b


            def example_register():
                fd = p9server.filesystem.create('test_func')
                p9server.register_function(communicate, fd)

        Communication using Twrite/Tread:

        .. testcode::

            import json


            async def example_write():
                fid = await p9client.fid('test_func')
                await p9client.write(
                    fid,
                    json.dumps({"a": 17, "b": 25})
                )
                msg = await p9client.read(fid)
                response = json.loads(msg['data'])
                assert response == 42

        Same, using a command line 9p client from plan9port::

            $ echo '{"a": 17, "b": 25}' | 9p -a localhost:8149 write test_func
            $ 9p -a localhost:8149 read test_func
            42

        And using a mounted file system via FUSE client from plan9port::

            $ 9pfuse localhost:8149 mnt
            $ echo '{"a": 17, "b": 25}' >mnt/test_func
            $ cat mnt/test_func
            42

        And the same, but using Tcall:

        .. testcode::

            async def example_call():
                fid = await p9client.fid('test_func')
                response = await p9client.call(fid, argv=(17, 25))
                assert response == 42

        And finnaly run this code:

        .. testcode::

            async def main():
                server_task = await p9server.async_run()
                example_register()
                await p9client.start_session()
                await example_write()
                await example_call()
                server_task.cancel()

            asyncio.run(main())
        '''
        return inode.register_function(func, loader, dumper)

    async def setup_endpoint(self):
        if getattr(self.local, 'server', None) is not None:
            return
        self.local.msg_queue = CoreMessageQueue(event_loop=self.event_loop)
        if self.status['use_socket']:
            self.local.server = None
            self.local.transport, self.local.protocol = (
                await self.event_loop.create_connection(
                    lambda: Plan9ServerProtocol(
                        self.connection_lost, self.marshal, self.filesystem
                    ),
                    sock=self.use_socket,
                )
            )
        else:
            self.local.transport = None
            self.local.protocol = None
            self.local.server = await self.event_loop.create_server(
                lambda: Plan9ServerProtocol(
                    self.connection_lost, self.marshal, self.filesystem
                ),
                *self.status['address'],
            )

    async def async_run(self):
        '''Return the server asyncio task.

        Using this task one can stop the server:

        .. testcode::

            async def main():
                server = Plan9ServerSocket(address=('localhost', 8149))
                server_task = await server.async_run()
                # ... server is running here
                server_task.cancel()
                # ... server is stopped

            asyncio.run(main())

        To forcefully close all client connections and stop the server
        immediately from a registered function, one can pass this task
        to the function, cancel it, and raise `Plan9Exit()` exception:

        .. testcode::

            import functools

            from pyroute2.plan9 import Plan9Exit

            server_sock, client_sock = socketpair()


            def test_exit_func(context):
                if 'server_task' in context:
                    context['server_task'].cancel()
                    raise Plan9Exit('server stopped upon client request')
                return 'server starting, please wait'


            async def server():
                p9server = Plan9ServerSocket(use_socket=server_sock)
                context = {}

                inode = p9server.filesystem.create('stop')
                p9server.register_function(
                    functools.partial(test_exit_func, context),
                    inode
                )
                context['server_task'] = await p9server.async_run()

                try:
                    await context['server_task']
                except asyncio.exceptions.CancelledError:
                    pass

                assert context['server_task'].cancelled()

        .. testcode::
            :hide:

            async def client():
                p9client = Plan9ClientSocket(use_socket=client_sock)
                await p9client.start_session()
                fid = await p9client.fid('stop')
                try:
                    await p9client.call(fid)
                except Plan9Exit:
                    pass


            async def main():
                await asyncio.gather(server(), client())

            asyncio.run(main())
        '''
        await self.setup_endpoint()
        if self.status['use_socket']:
            return self.protocol.on_con_lost
        else:
            return asyncio.create_task(self.local.server.serve_forever())

    def run(self):
        '''A simple synchronous runner.

        Uses `event_loop.run_forever()`.
        '''
        self.event_loop.create_task(self.async_run())
        self.event_loop.run_forever()
