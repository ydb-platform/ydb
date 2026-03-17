# Author:  Lisandro Dalcin
# Contact: dalcinl@gmail.com
"""Management of MPI worker processes."""
# pylint: disable=broad-except
# pylint: disable=too-many-lines
# pylint: disable=protected-access
# pylint: disable=missing-docstring
# pylint: disable=import-outside-toplevel

import os
import sys
import time
import atexit
import weakref
import warnings
import itertools
import threading
import collections

from .. import MPI
from ._core import BrokenExecutor


# ---


def serialized(function):
    if serialized.lock is None:
        return function
    def wrapper(*args, **kwargs):
        with serialized.lock:
            return function(*args, **kwargs)
    return wrapper
serialized.lock = None  # type: ignore[attr-defined]


def setup_mpi_threads():
    with setup_mpi_threads.lock:
        thread_level = setup_mpi_threads.thread_level
        if thread_level is None:
            thread_level = MPI.Query_thread()
            setup_mpi_threads.thread_level = thread_level
            if thread_level < MPI.THREAD_MULTIPLE:
                serialized.lock = threading.Lock()
    if thread_level < MPI.THREAD_SERIALIZED:
        warnings.warn("The level of thread support in MPI "
                      "should be at least MPI_THREAD_SERIALIZED",
                      RuntimeWarning, 2)
setup_mpi_threads.lock = threading.Lock()  # type: ignore[attr-defined]
setup_mpi_threads.thread_level = None      # type: ignore[attr-defined]


# ---


if sys.version_info[0] >= 3:

    def sys_exception():
        exc = sys.exc_info()[1]
        exc.__traceback__ = None
        return exc

else:  # pragma: no cover

    def sys_exception():
        exc = sys.exc_info()[1]
        return exc


def os_environ_get(name, default=None):
    varname = 'MPI4PY_FUTURES_{}'.format(name)
    if varname not in os.environ:
        oldname = 'MPI4PY_{}'.format(name)
        if oldname in os.environ:  # pragma: no cover
            message = "Environment variable {} is deprecated, use {}"
            warnings.warn(message.format(oldname, varname), DeprecationWarning)
            return os.environ[oldname]
    return os.environ.get(varname, default)


# ---


BACKOFF = 0.001


class Backoff:

    def __init__(self, seconds=BACKOFF):
        self.tval = 0.0
        self.tmax = max(float(seconds), 0.0)
        self.tmin = self.tmax / (1 << 10)

    def reset(self):
        self.tval = 0.0

    def sleep(self):
        time.sleep(self.tval)
        self.tval = min(self.tmax, max(self.tmin, self.tval * 2))


class Queue(collections.deque):
    put = collections.deque.append
    pop = collections.deque.popleft
    add = collections.deque.appendleft


class Stack(collections.deque):
    put = collections.deque.append
    pop = collections.deque.pop


THREADS_QUEUES = weakref.WeakKeyDictionary()  # type: weakref.WeakKeyDictionary


def join_threads(threads_queues=THREADS_QUEUES):
    items = list(threads_queues.items())
    for _, queue in items:   # pragma: no cover
        queue.put(None)
    for thread, _ in items:  # pragma: no cover
        thread.join()


try:
    threading._register_atexit(join_threads)  # type: ignore[attr-defined]
except AttributeError:  # pragma: no cover
    atexit.register(join_threads)


class Pool:

    def __init__(self, executor, manager, *args):
        self.size = None
        self.event = threading.Event()
        self.queue = queue = Queue()
        self.exref = weakref.ref(executor, lambda _, q=queue: q.put(None))

        args = (self, executor._options) + args
        thread = threading.Thread(target=manager, args=args)
        self.thread = thread

        setup_mpi_threads()
        try:
            threading._register_atexit
        except AttributeError:  # pragma: no cover
            thread.daemon = True
        thread.start()
        THREADS_QUEUES[thread] = queue

    def wait(self):
        self.event.wait()

    def push(self, item):
        self.queue.put(item)

    def done(self):
        self.queue.put(None)

    def join(self):
        self.thread.join()

    def setup(self, size):
        self.size = size
        self.event.set()
        return self.queue

    def cancel(self, handler=None):
        queue = self.queue
        while True:
            try:
                item = queue.pop()
            except LookupError:
                break
            if item is None:
                queue.put(None)
                break
            future, _ = item
            if handler:
                handler(future)
            else:
                future.cancel()
            del item, future

    def broken(self, message):
        lock = None
        executor = self.exref()
        if executor is not None:
            executor._broken = message
            if not executor._shutdown:
                lock = executor._lock

        def handler(future):
            if future.set_running_or_notify_cancel():
                exception = BrokenExecutor(message)
                future.set_exception(exception)

        self.event.set()
        if lock:
            lock.acquire()
        try:
            self.cancel(handler)
        finally:
            if lock:
                lock.release()


def initialize(options):
    initializer = options.pop('initializer', None)
    initargs = options.pop('initargs', ())
    initkwargs = options.pop('initkwargs', {})
    if initializer is not None:
        try:
            initializer(*initargs, **initkwargs)
            return True
        except BaseException:
            return False
    return True


def _manager_thread(pool, options):
    size = options.pop('max_workers', 1)
    queue = pool.setup(size)

    def init():
        if not initialize(options):
            pool.broken("initializer failed")
            return False
        return True

    def worker():
        backoff = Backoff(options.get('backoff', BACKOFF))
        if not init():
            queue.put(None)
            return
        while True:
            try:
                item = queue.pop()
                backoff.reset()
            except LookupError:
                backoff.sleep()
                continue
            if item is None:
                queue.put(None)
                break
            future, task = item
            if not future.set_running_or_notify_cancel():
                continue
            func, args, kwargs = task
            try:
                result = func(*args, **kwargs)
                future.set_result(result)
            except BaseException:
                exception = sys_exception()
                future.set_exception(exception)
            del item, future

    threads = [threading.Thread(target=worker) for _ in range(size - 1)]
    for thread in threads:
        thread.start()
    worker()
    for thread in threads:
        thread.join()
    queue.pop()


def _manager_comm(pool, options, comm, full=True):
    assert comm != MPI.COMM_NULL
    serialized(client_sync)(comm, options, full)
    if not client_init(comm, options):
        pool.broken("initializer failed")
        serialized(client_close)(comm)
        return
    size = comm.Get_remote_size()
    queue = pool.setup(size)
    workers = Stack(reversed(range(size)))
    client_exec(comm, options, 0, workers, queue)
    serialized(client_close)(comm)


def _manager_split(pool, options, comm, root):
    comm = serialized(comm_split)(comm, root)
    _manager_comm(pool, options, comm, full=False)


def _manager_spawn(pool, options):
    pyexe = options.pop('python_exe', None)
    pyargs = options.pop('python_args', None)
    nprocs = options.pop('max_workers', None)
    info = options.pop('mpi_info', None)
    comm = serialized(client_spawn)(pyexe, pyargs, nprocs, info)
    _manager_comm(pool, options, comm)


def _manager_service(pool, options):
    service = options.pop('service', None)
    info = options.pop('mpi_info', None)
    comm = serialized(client_connect)(service, info)
    _manager_comm(pool, options, comm)


def ThreadPool(executor):
    # pylint: disable=invalid-name
    return Pool(executor, _manager_thread)


def SplitPool(executor, comm, root):
    # pylint: disable=invalid-name
    return Pool(executor, _manager_split, comm, root)


def SpawnPool(executor):
    # pylint: disable=invalid-name
    return Pool(executor, _manager_spawn)


def ServicePool(executor):
    # pylint: disable=invalid-name
    return Pool(executor, _manager_service)


def WorkerPool(executor):
    # pylint: disable=invalid-name
    if SharedPool is not None:
        return SharedPool(executor)
    if 'service' in executor._options:
        return ServicePool(executor)
    else:
        return SpawnPool(executor)


# ---

SharedPool = None  # pylint: disable=invalid-name


def _set_shared_pool(obj):
    # pylint: disable=invalid-name
    # pylint: disable=global-statement
    global SharedPool
    SharedPool = obj


def _manager_shared(pool, options, comm, tag, workers):
    # pylint: disable=too-many-arguments
    if tag == 0:
        serialized(client_sync)(comm, options)
    if tag == 0:
        if not client_init(comm, options):
            pool.broken("initializer failed")
            return
    if tag >= 1:
        if options.get('initializer') is not None:
            pool.broken("cannot run initializer")
            return
    size = comm.Get_remote_size()
    queue = pool.setup(size)
    client_exec(comm, options, tag, workers, queue)


class SharedPoolCtx:
    # pylint: disable=too-few-public-methods

    def __init__(self):
        self.comm = MPI.COMM_NULL
        self.on_root = None
        self.counter = None
        self.workers = None
        self.threads = weakref.WeakKeyDictionary()

    def __call__(self, executor):
        assert SharedPool is self
        if self.comm != MPI.COMM_NULL and self.on_root:
            tag = next(self.counter)
            args = (self.comm, tag, self.workers)
            pool = Pool(executor, _manager_shared, *args)
        else:
            pool = Pool(executor, _manager_thread)
        del THREADS_QUEUES[pool.thread]
        self.threads[pool.thread] = pool.queue
        return pool

    def __enter__(self):
        assert SharedPool is None
        self.on_root = MPI.COMM_WORLD.Get_rank() == 0
        if MPI.COMM_WORLD.Get_size() >= 2:
            self.comm = comm_split(MPI.COMM_WORLD, root=0)
            if self.on_root:
                size = self.comm.Get_remote_size()
                self.counter = itertools.count(0)
                self.workers = Stack(reversed(range(size)))
        _set_shared_pool(self)
        return self if self.on_root else None

    def __exit__(self, *args):
        assert SharedPool is self
        if self.on_root:
            join_threads(self.threads)
        if self.comm != MPI.COMM_NULL:
            if self.on_root:
                if next(self.counter) == 0:
                    options = dict(main=False)
                    client_sync(self.comm, options)
                    client_init(self.comm, options)
                client_close(self.comm)
            else:
                options = server_sync(self.comm)
                server_init(self.comm)
                server_exec(self.comm, options)
                server_close(self.comm)
        if not self.on_root:
            join_threads(self.threads)
        _set_shared_pool(None)
        self.comm = MPI.COMM_NULL
        self.on_root = None
        self.counter = None
        self.workers = None
        self.threads.clear()
        return False


# ---


def barrier(comm):
    assert comm.Is_inter()
    try:
        request = comm.Ibarrier()
        backoff = Backoff()
        while not request.Test():
            backoff.sleep()
    except (NotImplementedError, MPI.Exception):  # pragma: no cover
        buf = [None, 0, MPI.BYTE]
        tag = MPI.COMM_WORLD.Get_attr(MPI.TAG_UB)
        sendreqs, recvreqs = [], []
        for pid in range(comm.Get_remote_size()):
            recvreqs.append(comm.Irecv(buf, pid, tag))
            sendreqs.append(comm.Issend(buf, pid, tag))
        backoff = Backoff()
        while not MPI.Request.Testall(recvreqs):
            backoff.sleep()
        MPI.Request.Waitall(sendreqs)


def bcast_send(comm, data):
    assert comm.Is_inter()
    assert comm.Get_size() == 1
    if MPI.VERSION >= 2:
        comm.bcast(data, MPI.ROOT)
    else:  # pragma: no cover
        tag = MPI.COMM_WORLD.Get_attr(MPI.TAG_UB)
        size = comm.Get_remote_size()
        MPI.Request.Waitall([
            comm.issend(data, pid, tag)
            for pid in range(size)])


def bcast_recv(comm):
    assert comm.Is_inter()
    assert comm.Get_remote_size() == 1
    if MPI.VERSION >= 2:
        data = comm.bcast(None, 0)
    else:  # pragma: no cover
        tag = MPI.COMM_WORLD.Get_attr(MPI.TAG_UB)
        data = comm.recv(None, 0, tag)
    return data


# ---


def client_sync(comm, options, full=True):
    assert comm.Is_inter()
    assert comm.Get_size() == 1
    barrier(comm)
    if full:
        options = _sync_get_data(options)
    bcast_send(comm, options)


def client_init(comm, options):
    serialized(bcast_send)(comm, _init_get_data(options))
    sbuf = bytearray([False])
    rbuf = bytearray([False])
    serialized(comm.Allreduce)(sbuf, rbuf, op=MPI.LAND)
    success = bool(rbuf[0])
    return success


def client_exec(comm, options, tag, worker_pool, task_queue):
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-statements
    assert comm.Is_inter()
    assert comm.Get_size() == 1
    assert tag >= 0

    backoff = Backoff(options.get('backoff', BACKOFF))

    status = MPI.Status()
    comm_recv = serialized(comm.recv)
    comm_isend = serialized(comm.issend)
    comm_iprobe = serialized(comm.iprobe)
    request_free = serialized(MPI.Request.Free)

    pending = {}

    def iprobe():
        pid = MPI.ANY_SOURCE
        return comm_iprobe(pid, tag, status)

    def probe():
        pid = MPI.ANY_SOURCE
        backoff.reset()
        while not comm_iprobe(pid, tag, status):
            backoff.sleep()

    def recv():
        pid = MPI.ANY_SOURCE
        try:
            task = comm_recv(None, pid, tag, status)
        except BaseException:
            task = (None, sys_exception())
        pid = status.source
        worker_pool.put(pid)

        future, request = pending.pop(pid)
        request_free(request)
        result, exception = task
        if exception is None:
            future.set_result(result)
        else:
            future.set_exception(exception)

    def send():
        item = task_queue.pop()
        if item is None:
            return True

        try:
            pid = worker_pool.pop()
        except LookupError:  # pragma: no cover
            task_queue.add(item)
            return False

        future, task = item
        if not future.set_running_or_notify_cancel():
            worker_pool.put(pid)
            return False

        try:
            request = comm_isend(task, pid, tag)
            pending[pid] = (future, request)
        except BaseException:
            worker_pool.put(pid)
            future.set_exception(sys_exception())
        return None

    while True:
        if task_queue and worker_pool:
            backoff.reset()
            stop = send()
            if stop:
                break
        if pending and iprobe():
            backoff.reset()
            recv()
        backoff.sleep()
    while pending:
        probe()
        recv()


def client_close(comm):
    assert comm.Is_inter()
    MPI.Request.Waitall([
        comm.issend(None, dest=pid, tag=0)
        for pid in range(comm.Get_remote_size())])
    try:
        comm.Disconnect()
    except NotImplementedError:  # pragma: no cover
        comm.Free()


def server_sync(comm, full=True):
    assert comm.Is_inter()
    assert comm.Get_remote_size() == 1
    barrier(comm)
    options = bcast_recv(comm)
    if full:
        options = _sync_set_data(options)
    return options


def server_init(comm):
    options = bcast_recv(comm)
    success = initialize(options)
    sbuf = bytearray([success])
    rbuf = bytearray([True])
    comm.Allreduce(sbuf, rbuf, op=MPI.LAND)
    assert bool(rbuf[0]) is False
    return success


def server_exec(comm, options):
    assert comm.Is_inter()
    assert comm.Get_remote_size() == 1

    backoff = Backoff(options.get('backoff', BACKOFF))

    status = MPI.Status()
    comm_recv = comm.recv
    comm_isend = comm.issend
    comm_iprobe = comm.iprobe
    request_test = MPI.Request.Test

    def recv():
        pid, tag = MPI.ANY_SOURCE, MPI.ANY_TAG
        backoff.reset()
        while not comm_iprobe(pid, tag, status):
            backoff.sleep()
        pid, tag = status.source, status.tag
        try:
            task = comm_recv(None, pid, tag, status)
        except BaseException:
            task = sys_exception()
        return task

    def call(task):
        if isinstance(task, BaseException):
            return (None, task)
        func, args, kwargs = task
        try:
            result = func(*args, **kwargs)
            return (result, None)
        except BaseException:
            return (None, sys_exception())

    def send(task):
        pid, tag = status.source, status.tag
        try:
            request = comm_isend(task, pid, tag)
        except BaseException:
            task = (None, sys_exception())
            request = comm_isend(task, pid, tag)
        backoff.reset()
        while not request_test(request):
            backoff.sleep()

    while True:
        task = recv()
        if task is None:
            break
        task = call(task)
        send(task)


def server_close(comm):
    try:
        comm.Disconnect()
    except NotImplementedError:  # pragma: no cover
        comm.Free()


# ---


def get_comm_world():
    return MPI.COMM_WORLD


def comm_split(comm, root=0):
    assert not comm.Is_inter()
    assert comm.Get_size() > 1
    assert 0 <= root < comm.Get_size()
    rank = comm.Get_rank()

    if MPI.Get_version() >= (2, 2):
        allgroup = comm.Get_group()
        if rank == root:
            group = allgroup.Incl([root])
        else:
            group = allgroup.Excl([root])
        allgroup.Free()
        intracomm = comm.Create(group)
        group.Free()
    else:  # pragma: no cover
        color = 0 if rank == root else 1
        intracomm = comm.Split(color, key=0)

    if rank == root:
        local_leader = 0
        remote_leader = 0 if root else 1
    else:
        local_leader = 0
        remote_leader = root
    intercomm = intracomm.Create_intercomm(
        local_leader, comm, remote_leader, tag=0)
    intracomm.Free()
    return intercomm


# ---


MAIN_RUN_NAME = '__worker__'


def import_main(mod_name, mod_path, init_globals, run_name):
    import types
    import runpy

    module = types.ModuleType(run_name)
    if init_globals is not None:
        module.__dict__.update(init_globals)
        module.__name__ = run_name

    class TempModulePatch(runpy._TempModule):
        # pylint: disable=too-few-public-methods
        def __init__(self, mod_name):
            # pylint: disable=no-member
            super().__init__(mod_name)
            assert self.module.__name__ == run_name
            self.module = module

    TempModule = runpy._TempModule  # pylint: disable=invalid-name
    runpy._TempModule = TempModulePatch
    import_main.sentinel = (mod_name, mod_path)
    main_module = sys.modules['__main__']
    try:
        sys.modules['__main__'] = sys.modules[run_name] = module
        if mod_name:  # pragma: no cover
            runpy.run_module(mod_name, run_name=run_name, alter_sys=True)
        elif mod_path:  # pragma: no branch
            if not getattr(sys.flags, 'isolated', 0):  # pragma: no branch
                sys.path[0] = os.path.realpath(os.path.dirname(mod_path))
            runpy.run_path(mod_path, run_name=run_name)
        sys.modules['__main__'] = sys.modules[run_name] = module
    except:  # pragma: no cover
        sys.modules['__main__'] = main_module
        raise
    finally:
        del import_main.sentinel
        runpy._TempModule = TempModule


def _sync_get_data(options):
    main = sys.modules['__main__']
    sys.modules.setdefault(MAIN_RUN_NAME, main)
    import_main_module = options.pop('main', True)

    data = options.copy()
    data.pop('initializer', None)
    data.pop('initargs', None)
    data.pop('initkwargs', None)

    if import_main_module:
        if sys.version_info[0] >= 3:
            spec = getattr(main, '__spec__', None)
            name = getattr(spec, 'name', None)
        else:  # pragma: no cover
            loader = getattr(main, '__loader__', None)
            name = getattr(loader, 'fullname', None)
        path = getattr(main, '__file__', None)
        if name is not None:  # pragma: no cover
            data['@main:mod_name'] = name
        if path is not None:  # pragma: no branch
            data['@main:mod_path'] = path

    return data


def _sync_set_data(data):
    if 'path' in data:
        sys.path.extend(data.pop('path'))
    if 'wdir' in data:
        os.chdir(data.pop('wdir'))
    if 'env' in data:
        os.environ.update(data.pop('env'))

    mod_name = data.pop('@main:mod_name', None)
    mod_path = data.pop('@main:mod_path', None)
    mod_glbs = data.pop('globals', None)
    import_main(mod_name, mod_path, mod_glbs, MAIN_RUN_NAME)

    return data


def _init_get_data(options):
    keys = ('initializer', 'initargs', 'initkwargs')
    vals = (None, (), {})
    data = dict((k, options.pop(k, v)) for k, v in zip(keys, vals))
    return data


# ---


def _check_recursive_spawn():  # pragma: no cover
    if not hasattr(import_main, 'sentinel'):
        return
    main_name, main_path = import_main.sentinel
    main_info = "\n"
    if main_name is not None:
        main_info += "    main name: '{}'\n".format(main_name)
    if main_path is not None:
        main_info += "    main path: '{}'\n".format(main_path)
    main_info += "\n"
    sys.stderr.write("""
    The main script or module attempted to spawn new MPI worker processes.
    This probably means that you have forgotten to use the proper idiom in
    your main script or module:

        if __name__ == '__main__':
            ...

    This error is unrecoverable. The MPI execution environment had to be
    aborted. The name/path of the offending main script/module follows:
    """ + main_info)
    sys.stderr.flush()
    time.sleep(1)
    MPI.COMM_WORLD.Abort(1)


FLAG_OPT_MAP = {
    # Python 3
    'inspect': 'i',
    'interactive': 'i',
    'debug': 'd',
    'optimize': 'O',
    'no_user_site': 's',
    'no_site': 'S',
    'isolated': 'I',
    'ignore_environment': 'E',
    'dont_write_bytecode': 'B',
    'hash_randomization': 'R',
    'verbose': 'v',
    'quiet': 'q',
    'bytes_warning': 'b',
    # 'dev_mode': 'Xdev',
    # 'utf8_mode': 'Xutf8',
    # 'warn_default_encoding': 'Xwarn_default_encoding',
    # Python 2
    'division_warning': 'Qwarn',
    'division_new': 'Qnew',
    'py3k_warning': '3',
    'tabcheck': 't',
    'unicode': 'U',
}


def get_python_flags():
    args = []
    for flag, opt in FLAG_OPT_MAP.items():
        val = getattr(sys.flags, flag, 0)
        val = val if opt[0] != 'i' else 0
        val = val if opt[0] != 'Q' else min(val, 1)
        if val > 0:
            args.append('-' + opt * val)
    for opt in sys.warnoptions:  # pragma: no cover
        args.append('-W' + opt)
    sys_xoptions = getattr(sys, '_xoptions', {})
    for opt, val in sys_xoptions.items():  # pragma: no cover
        args.append('-X' + opt if val is True else
                    '-X' + opt + '=' + val)
    return args


def get_max_workers():
    max_workers = os_environ_get('MAX_WORKERS')
    if max_workers is not None:
        return int(max_workers)
    if MPI.UNIVERSE_SIZE != MPI.KEYVAL_INVALID:  # pragma: no branch
        universe_size = MPI.COMM_WORLD.Get_attr(MPI.UNIVERSE_SIZE)
        if universe_size is not None:  # pragma: no cover
            world_size = MPI.COMM_WORLD.Get_size()
            return max(universe_size - world_size, 1)
    return 1


def get_spawn_module():
    return __spec__.parent + '.server'


def client_spawn(python_exe=None,
                 python_args=None,
                 max_workers=None,
                 mpi_info=None):
    _check_recursive_spawn()
    if python_exe is None:
        python_exe = sys.executable
    if python_args is None:
        python_args = []
    if max_workers is None:
        max_workers = get_max_workers()
    if mpi_info is None:
        mpi_info = dict(soft='1:{}'.format(max_workers))

    args = get_python_flags() + list(python_args)
    args.extend(['-m', get_spawn_module()])
    info = MPI.Info.Create()
    info.update(mpi_info)
    comm = MPI.COMM_SELF.Spawn(python_exe, args, max_workers, info)
    info.Free()
    return comm


# ---


SERVICE = __spec__.parent
SERVER_HOST = 'localhost'
SERVER_BIND = ''
SERVER_PORT = 31415


def get_service():
    return os_environ_get('SERVICE', SERVICE)


def get_server_host():
    return os_environ_get('SERVER_HOST', SERVER_HOST)


def get_server_bind():
    return os_environ_get('SERVER_BIND', SERVER_BIND)


def get_server_port():
    return int(os_environ_get('SERVER_PORT', SERVER_PORT))


def client_lookup(address):
    from socket import socket
    host, port = address
    host = host or get_server_host()
    port = port or get_server_port()
    address = (host, int(port))
    sock = socket()
    sock.connect(address)
    try:
        fdes = sock.fileno()  # pylint: disable=no-member
        peer = MPI.Comm.Join(fdes)
    finally:
        sock.close()
    mpi_port = peer.recv(None, 0)
    peer.Disconnect()
    return mpi_port


def server_publish(address, mpi_port):
    from socket import socket
    from socket import SOL_SOCKET, SO_REUSEADDR
    host, port = address
    host = host or get_server_bind()
    port = port or get_server_port()
    address = (host, int(port))
    serversock = socket()
    serversock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serversock.bind(address)
    serversock.listen(0)
    try:
        sock = serversock.accept()[0]
    finally:
        serversock.close()
    try:
        fdes = sock.fileno()  # pylint: disable=no-member
        peer = MPI.Comm.Join(fdes)
    finally:
        sock.close()
    peer.send(mpi_port, 0)
    peer.Disconnect()


def client_connect(service, mpi_info=None):
    info = MPI.INFO_NULL
    if mpi_info:
        info = MPI.Info.Create()
        info.update(mpi_info)
    if not isinstance(service, (list, tuple)):
        service = service or get_service()
        port = MPI.Lookup_name(service, info)
    else:
        port = client_lookup(service)

    comm = MPI.COMM_SELF.Connect(port, info, root=0)
    if info != MPI.INFO_NULL:
        info.Free()
    return comm


def server_accept(service, mpi_info=None,
                  root=0, comm=MPI.COMM_WORLD):
    assert not comm.Is_inter()
    assert 0 <= root < comm.Get_size()

    info = MPI.INFO_NULL
    if comm.Get_rank() == root:
        if mpi_info:
            info = MPI.Info.Create()
            info.update(mpi_info)
    port = None
    if comm.Get_rank() == root:
        port = MPI.Open_port(info)
    if comm.Get_rank() == root:
        if not isinstance(service, (list, tuple)):
            service = service or get_service()
            MPI.Publish_name(service, port, info)
        else:
            server_publish(service, port)
            service = None

    comm = comm.Accept(port, info, root)
    if port is not None:
        if service is not None:
            MPI.Unpublish_name(service, port, info)
        MPI.Close_port(port)
    if info != MPI.INFO_NULL:
        info.Free()
    return comm


# ---


def server_main_comm(comm, full=True):
    assert comm != MPI.COMM_NULL
    options = server_sync(comm, full)
    server_init(comm)
    server_exec(comm, options)
    server_close(comm)


def server_main_split(comm, root):
    comm = comm_split(comm, root)
    server_main_comm(comm, full=False)


def server_main_spawn():
    comm = MPI.Comm.Get_parent()
    server_main_comm(comm)


def server_main_service():
    from getopt import getopt
    longopts = ['bind=', 'port=', 'service=', 'info=']
    optlist, _ = getopt(sys.argv[1:], '', longopts)
    optdict = {opt[2:]: val for opt, val in optlist}

    if 'bind' in optdict or 'port' in optdict:
        bind = optdict.get('bind') or get_server_bind()
        port = optdict.get('port') or get_server_port()
        service = (bind, int(port))
    else:
        service = optdict.get('service') or get_service()
    info = optdict.get('info', '').split(',')
    info = dict(k_v.split('=', 1) for k_v in info if k_v)

    comm = server_accept(service, info)
    server_main_comm(comm)


def server_main():
    from ..run import set_abort_status
    try:
        comm = MPI.Comm.Get_parent()
        if comm != MPI.COMM_NULL:
            server_main_spawn()
        else:
            server_main_service()
    except:
        set_abort_status(1)
        raise


# ---
