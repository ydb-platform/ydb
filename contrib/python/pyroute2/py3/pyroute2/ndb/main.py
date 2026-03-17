'''
.. testsetup:: *

    from pyroute2 import config
    from pyroute2 import NDB

    config.mock_netlink = True
    ndb = NDB()

.. testcleanup:: *

    for key, value in tuple(globals().items()):
        if key.startswith('ndb') and hasattr(value, 'close'):
            value.close()

NDB is a high level network management module. IT allows to manage interfaces,
routes, addresses etc. of connected systems, containers and network
namespaces.

In a nutshell, NDB collects and aggregates netlink events in an SQL database,
provides Python objects to reflect the system state, and applies changes back
to the system. The database expects updates only from the sources, no manual
SQL updates are expected normally.

.. aafig::
    :scale: 80
    :textual:

        +----------------------------------------------------------------+
      +----------------------------------------------------------------+ |
    +----------------------------------------------------------------+ | |
    |                                                                | | |
    |                              kernel                            | |-+
    |                                                                |-+
    +----------------------------------------------------------------+
            |                      | ^                     | ^
            | `netlink events`     | |                     | |
            |                      | |                     | |
            v                      v |                     v |
     +--------------+        +--------------+        +--------------+
     |   `async`    |        |   `async`    |        |   `async`    |
     |   `source`   |        |   `source`   |        |   `source`   |<-------\\
     +--------------+        +--------------+        +--------------+        |
            |                       |                       |                |
            |                       |                       |                |
            \\-----------------------+-----------------------/                |
                                    |                                        |
              parsed netlink events |                                        |
                                    |                                        |
                                    v                                        |
                     +-----------------------------+                         |
                     |   `Source.receiver()`       |                         |
                     |    async task               |                         |
                     +-----------------------------+                         |
                                    |                                        |
                                    |  `TaskManager.event_queue`             |
                                    |                                        |
                                    v                                        |
                     +-----------------------------+                         |
                     | `TaskManager.receiver()`    |                         |
                     | `TaskManager.task_watch()`  |                         |
                     +-----------------------------+                         |
                                    |                                        |
                                    |  `AsyncObject: load_...msg()`          |
                                    |  `Schema.load_netlink()`               |
                                    v                                        |
                     +-----------------------------+                         |
                     |  SQL database               |                         |
                     |     `sqlite3 :memory:`      |                         |
                     +-----------------------------+                         |
                                    |                                        |
                                    |                                        |
                                    v                                        |
                         +-------------------------+                         |
                       +-------------------------+ |                         |
                     +-------------------------+ | |  `AsyncObject.apply()`  |
                     | `AsyncObject:`          | | |-------------------------/
                     |  `interface`            | | |
                     |  `address`              | | |
                     |  `route`                | |-+
                     |  `...`                  |-+
                     +-------------------------+
                                   ^
                                   |
                                     `asynchronous API, AsyncNDB thread`
                                   |

     - - - - - - - - - - - - - - - + - - - - - - - - - - - - - - - - -

                                   |
                                     `synchronous API, MainThread`
                                   |
     `sync_api.SyncView`           v
     `sync_api.SyncBase` +-------------------------+
                       +-------------------------+ |
                     +-------------------------+ | |
                     | `RTNL_Object:`          | | |
                     |  `interface`            | | |
                     |  `address`              | | |
                     |  `route`                | |-+
                     |  `...`                  |-+
                     +-------------------------+

.. container:: aafig-caption

    NDB architecture diagram

The goal of NDB is to provide an easy access to RTNL info and entities via
Python objects, like `pyroute2.ndb.objects.interface` (see also:
:ref:`ndbinterfaces`), `pyroute2.ndb.objects.route` (see also:
:ref:`ndbroutes`) etc. These objects do not
only reflect the system state for the time of their instantiation, but
continuously monitor the system for relevant updates. The monitoring is
done via netlink notifications, thus no polling. Also the objects allow
to apply changes back to the system and rollback the changes.

On the other hand it's too expensive to create Python objects for all the
available RTNL entities, e.g. when there are hundreds of interfaces and
thousands of routes. Thus NDB creates objects only upon request, when
the user calls `.create()` to create new objects or runs
`ndb.<view>[selector]` (e.g. `ndb.interfaces['eth0']`) to access an
existing object.

To list existing RTNL entities NDB uses objects of the class `RecordSet`
that `yield` individual `Record` objects for every entity (see also:
:ref:`ndbreports`). An object of the `Record` class is immutable, doesn't
monitor any updates, doesn't contain any links to other objects and essentially
behaves like a simple named tuple.

.. aafig::
    :scale: 80
    :textual:


      +---------------------+
      |                     |
      |                     |
      | `NDB() instance`    |
      |                     |
      |                     |
      +---------------------+
                 |
                 |
        +-------------------+
      +-------------------+ |
    +-------------------+ | |-----------+--------------------------+
    |                   | | |           |                          |
    |                   | | |           |                          |
    | `View()`          | | |           |                          |
    |                   | |-+           |                          |
    |                   |-+             |                          |
    +-------------------+               |                          |
                               +------------------+       +------------------+
                               |                  |       |                  |
                               |                  |       |                  |
                               | `.dump()`        |       | `.create()`      |
                               | `.summary()`     |       | `.__getitem__()` |
                               |                  |       |                  |
                               |                  |       |                  |
                               +------------------+       +------------------+
                                        |                           |
                                        |                           |
                                        v                           v
                              +-------------------+        +------------------+
                              |                   |      +------------------+ |
                              |                   |    +------------------+ | |
                              | `RecordSet()`     |    | `Interface()`    | | |
                              |                   |    | `Address()`      | | |
                              |                   |    | `Route()`        | | |
                              +-------------------+    | `Neighbour()`    | | |
                                        |              | `Rule()`         | |-+
                                        |              |  ...             |-+
                                        v              +------------------+
                                +-------------------+
                              +-------------------+ |
                            +-------------------+ | |
                            | `filter()`        | | |
                            | `select()`        | | |
                            | `transform()`     | | |
                            | `join()`          | |-+
                            |  ...              |-+
                            +-------------------+
                                        |
                                        v
                                +-------------------+
                              +-------------------+ |
                            +-------------------+ | |
                            |                   | | |
                            |                   | | |
                            | `Record()`        | | |
                            |                   | |-+
                            |                   |-+
                            +-------------------+

.. container:: aafig-caption

    NDB: synchronous API

Here are some simple NDB usage examples. More info see in the reference
documentation below.

Print all the interface names on the system, assume we have an NDB
instance `ndb`:

.. testcode::

    for interface in ndb.interfaces.dump():
        print(interface.ifname)

.. testoutput::

    lo
    eth0

Print the routing information in the CSV format:

.. testcode::

    for record in ndb.routes.summary().format('csv'):
        print(record)

.. testoutput::

    'target','tflags','table','ifname','dst','dst_len','gateway'
    'localhost',0,254,'eth0','',0,'192.168.122.1'
    'localhost',0,254,'eth0','192.168.122.0',24,
    'localhost',0,255,'lo','127.0.0.0',8,
    'localhost',0,255,'lo','127.0.0.1',32,
    'localhost',0,255,'lo','127.255.255.255',32,
    'localhost',0,255,'eth0','192.168.122.28',32,
    'localhost',0,255,'eth0','192.168.122.255',32,

.. note:: More on report filtering and formatting: :ref:`ndbreports`

Print IP addresses of interfaces in several network namespaces as:

.. testcode:: netns

    nslist = ['netns01',
              'netns02',
              'netns03']

    for nsname in nslist:
        ndb.sources.add(netns=nsname)

    report = ndb.addresses.summary()
    report.select_records(target=lambda x: x.startswith('netns'))
    report.select_fields('address', 'ifname', 'target')
    for line in report.format('json'):
        print(line)

.. testoutput:: netns

    [
        {
            "address": "127.0.0.1",
            "ifname": "lo",
            "target": "netns01"
        },
        {
            "address": "127.0.0.1",
            "ifname": "lo",
            "target": "netns02"
        },
        {
            "address": "127.0.0.1",
            "ifname": "lo",
            "target": "netns03"
        }
    ]

Add an IP address on an interface:

.. testcode::

    with ndb.interfaces['eth0'] as eth0:
        eth0.add_ip('10.0.0.1/24')
    # ---> <---  NDB waits until the address setup

Change an interface property:

.. testcode::

    with ndb.interfaces['eth0'] as eth0:
        eth0.set(
            state='up',
            address='00:11:22:33:44:55',
        )
    # ---> <---  NDB waits here for the changes to be applied
    #            the commit() is called automatically by the
    #            context manager's __exit__()

'''

import atexit
import ctypes
import ctypes.util
import inspect
import logging
import logging.handlers
import threading
from dataclasses import dataclass
from functools import reduce
from urllib.parse import urlparse

from pyroute2.common import basestring

##
# NDB stuff
from .objects import RTNL_Object
from .objects.interface import SyncInterface
from .objects.route import SyncRoute
from .source import SyncSource
from .sync_api import Flags, SyncDB, SyncSources, SyncView
from .task_manager import TaskManager
from .transaction import Transaction
from .view import SourcesView, View

log = logging.getLogger(__name__)


class Log:
    def __init__(self, log_id=None):
        self.logger = None
        self.state = False
        self.log_id = log_id or id(self)
        self.logger = logging.getLogger('pyroute2.ndb.%s' % self.log_id)
        self.main = self.channel('main')

    def __call__(self, target=None, level=logging.INFO):
        if target is None:
            return self.logger is not None

        if self.logger is not None:
            for handler in tuple(self.logger.handlers):
                self.logger.removeHandler(handler)

        if target in ('off', False):
            if self.state:
                self.logger.setLevel(0)
                self.logger.addHandler(logging.NullHandler())
            return

        if target in ('on', 'stderr'):
            handler = logging.StreamHandler()
        elif target == 'debug':
            handler = logging.StreamHandler()
            level = logging.DEBUG
        elif isinstance(target, basestring):
            url = urlparse(target)
            if not url.scheme and url.path:
                handler = logging.FileHandler(url.path)
            elif url.scheme == 'syslog':
                handler = logging.handlers.SysLogHandler(
                    address=url.netloc.split(':')
                )
            else:
                raise ValueError('logging scheme not supported')
        else:
            handler = target

        # set formatting only for new created logging handlers
        if handler is not target:
            fmt = '%(asctime)s %(levelname)8s %(name)s: %(message)s'
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)

        self.logger.addHandler(handler)
        self.logger.setLevel(level)

    @property
    def on(self):
        self.__call__(target='on')

    @property
    def off(self):
        self.__call__(target='off')

    def close(self):
        manager = self.logger.manager
        name = self.logger.name
        # the loggerDict can be huge, so don't
        # cache all the keys -- cache only the
        # needed ones
        purge_list = []
        for logger in manager.loggerDict.keys():
            if logger.startswith(name):
                purge_list.append(logger)
        # now shoot them one by one
        for logger in purge_list:
            del manager.loggerDict[logger]
        # don't force GC, leave it to the user
        del manager
        del name
        del purge_list

    def channel(self, name):
        return logging.getLogger('pyroute2.ndb.%s.%s' % (self.log_id, name))

    def callstack(self, *argv, **kwarg):
        # frame 0: self.debug()
        # frame 1: the function that sends the logging
        # frame 2: the caller function
        self.main.debug(
            'call stack: %s:%s %s() -> %s:%s %s()',
            *reduce(
                lambda x, y: x + y,
                reversed(
                    [
                        (x.filename, x.lineno, x.function)
                        for x in inspect.stack()[1:3]
                    ]
                ),
            ),
        )
        return self.debug(*argv, **kwarg)

    def debug(self, *argv, **kwarg):
        return self.main.debug(*argv, **kwarg)

    def info(self, *argv, **kwarg):
        return self.main.info(*argv, **kwarg)

    def warning(self, *argv, **kwarg):
        return self.main.warning(*argv, **kwarg)

    def error(self, *argv, **kwarg):
        return self.main.error(*argv, **kwarg)

    def critical(self, *argv, **kwarg):
        return self.main.critical(*argv, **kwarg)


@dataclass
class NDBConfig:
    localhost: str = 'localhost'
    rtnl_debug: bool = False
    show_format: str = 'native'
    db_spec: str = ':memory:'


class NDB:

    def __init__(
        self,
        sources=None,
        localhost='localhost',
        db_provider=None,
        db_spec=':memory:',
        db_cleanup=True,
        rtnl_debug=False,
        log=False,
        auto_netns=False,
        libc=None,
    ):
        self.libc = libc or ctypes.CDLL(
            ctypes.util.find_library('c'), use_errno=True
        )
        self.log = Log(log_id=id(self))
        self._dbm_thread = None
        self._dbm_ready = threading.Event()
        self._dbm_shutdown = threading.Event()
        #
        if log:
            if isinstance(log, basestring):
                self.log(log)
            elif isinstance(log, (tuple, list)):
                self.log(*log)
            elif isinstance(log, dict):
                self.log(**log)
            else:
                raise TypeError('wrong log spec format')
        #
        # fix sources prime
        if sources is None:
            sources = [
                {'target': localhost, 'kind': 'local', 'nlm_generator': 1}
            ]
        elif not isinstance(sources, (list, tuple)):
            raise ValueError('sources format not supported')

        for spec in sources:
            if 'target' not in spec:
                spec['target'] = localhost
                break

        self._nl = sources
        atexit.register(self.close)
        self._dbm_ready.clear()
        self._dbm_error = None
        self.config = NDBConfig(
            localhost=localhost, db_spec=db_spec, rtnl_debug=rtnl_debug
        )
        #
        self.task_manager = TaskManager(self)
        #
        self._dbm_thread = threading.Thread(
            target=self.task_manager.main, name='NDB main loop'
        )
        self._dbm_thread.daemon = True
        self._dbm_thread.start()
        self._dbm_ready.wait()
        for vname, view in self._create_views():
            setattr(self, vname, view)
        self.db = SyncDB(self.task_manager.event_loop, self)
        for spec in self._nl:
            spec['event'] = None
            self.sources.add(**spec)
        if self._dbm_error is not None:
            raise self._dbm_error

    @property
    def localhost(self):
        return self.config.localhost

    @localhost.setter
    def localhost(self, value):
        self.config.localhost = value

    def _create_views(self, flags=Flags.UNSPEC):
        views_map = (
            ('interfaces', 'interfaces', View, SyncView),
            ('addresses', 'addresses', View, SyncView),
            ('routes', 'routes', View, SyncView),
            ('neighbours', 'neighbours', View, SyncView),
            ('af_bridge_fdb', 'fdb', View, SyncView),
            ('rules', 'rules', View, SyncView),
            ('netns', 'netns', View, SyncView),
            ('probes', 'probes', View, SyncView),
            ('af_bridge_vlans', 'vlans', View, SyncView),
            ('sources', 'sources', SourcesView, SyncSources),
        )
        class_map = {
            'interfaces': SyncInterface,
            'routes': SyncRoute,
            'sources': SyncSource,
            'default': RTNL_Object,
        }
        ret = {}
        for vtable, vname, vclass, sync_vclass in views_map:
            view = vclass(self, vtable)
            sview = sync_vclass(
                self.task_manager.event_loop, view, class_map, flags=flags
            )
            ret[vname] = sview
        return iter(ret.items())

    def _get_view(self, table, chain=None):
        return View(self, table, chain)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def begin(self):
        return Transaction(self.log.channel('transaction'))

    def readonly(self):
        class AuthProxy:
            pass

        ap = AuthProxy()
        for vname, view in self._create_views(flags=Flags.RO):
            setattr(ap, vname, view)
        return ap

    def auth_proxy(self, auth_manager):
        raise NotImplementedError()

    def close(self):
        if self._dbm_shutdown.is_set():
            return
        else:
            self._dbm_shutdown.set()
        if hasattr(atexit, 'unregister'):
            atexit.unregister(self.close)
        else:
            try:
                atexit._exithandlers.remove((self.close, (), {}))
            except ValueError:
                pass
        # shutdown the _dbm_thread
        self.task_manager.event_loop.call_soon_threadsafe(
            self.task_manager.stop_event.set
        )
        self._dbm_shutdown.wait()
        self._dbm_thread.join()
        # shutdown the logger -- free the resources
        self.log.close()

    def backup(self, spec):
        return self.db.backup(spec)

    def reload(self, kinds=None):
        for source in self.sources.values():
            if kinds is not None and source.kind in kinds:
                source.restart()
