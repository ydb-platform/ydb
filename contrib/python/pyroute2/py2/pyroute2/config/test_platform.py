'''
Platform tests to discover the system capabilities.
'''
import os
import sys
import select
import struct
import threading
from pyroute2 import config
from pyroute2.common import uifname
from pyroute2 import RawIPRoute
from pyroute2.netlink.rtnl import RTMGRP_LINK


class SkipTest(Exception):
    pass


class TestCapsRtnl(object):
    '''
    A minimal test set to collect the RTNL implementation
    capabilities.

    It uses raw RTNL sockets and doesn't run any proxy code, so
    no transparent helpers are executed -- e.g., it will not
    create bridge via `brctl`, if RTNL doesn't support it.

    A short developer's guide::

        def test_whatever_else(self):
            code

    This test will create a capability record `whatever_else`. If
    the `code` fails, the `whatever_else` will be set to `False`.
    If it throws the `SkipTest` exception, the `whatever_else` will
    be set to `None`. Otherwise it will be set to whatever the test
    returns.

    To collect the capabilities::

        tce = TestCapsExt()
        tce.collect()
        print(tce.capabilities)

    Collected capabilities are in the `TestCapsExt.capabilities`
    dictionary, you can use them directly or by setting the
    `config.capabilities` singletone::

        from pyroute2 import config
        # ...
        tce.collect()
        config.capabilities = tce.capabilities
    '''

    def __init__(self):
        self.capabilities = {}
        self.ifnames = []
        self.rtm_newlink = {}
        self.rtm_dellink = {}
        self.rtm_events = {}
        self.cmd, self.cmdw = os.pipe()
        self.ip = None
        self.event = threading.Event()

    def __getitem__(self, key):
        return self.capabilities[key]

    def set_capability(self, key, value):
        '''
        Set a capability.
        '''
        self.capabilities[key] = value

    def ifname(self):
        '''
        Register and return a new unique interface name to
        be used in a test.
        '''
        ifname = uifname()
        self.ifnames.append(ifname)
        self.rtm_events[ifname] = threading.Event()
        self.rtm_newlink[ifname] = []
        self.rtm_dellink[ifname] = []
        return ifname

    def monitor(self):
        # The monitoring code to collect RTNL messages
        # asynchronously.
        # Do **NOT** run manually.

        # use a separate socket for monitoring
        ip = RawIPRoute()
        ip.bind(RTMGRP_LINK)
        poll = select.poll()
        poll.register(ip, select.POLLIN | select.POLLPRI)
        poll.register(self.cmd, select.POLLIN | select.POLLPRI)
        self.event.set()
        while True:
            events = poll.poll()
            for (fd, evt) in events:
                if fd == ip.fileno():
                    msgs = ip.get()
                    for msg in msgs:
                        name = msg.get_attr('IFLA_IFNAME')
                        event = msg.get('event')
                        if name not in self.rtm_events:
                            continue
                        if event == 'RTM_NEWLINK':
                            self.rtm_events[name].set()
                            self.rtm_newlink[name].append(msg)
                        elif event == 'RTM_DELLINK':
                            self.rtm_dellink[name].append(msg)
                else:
                    ip.close()
                    return

    def setup(self):
        # The setup procedure for a test.
        # Do **NOT** run manually.

        # create the raw socket
        self.ip = RawIPRoute()

    def teardown(self):
        # The teardown procedure for a test.
        # Do **NOT** run manually.

        # clear the collected interfaces
        for ifname in self.ifnames:
            self.rtm_events[ifname].wait()
            self.rtm_events[ifname].clear()
            if self.rtm_newlink.get(ifname):
                self.ip.link('del', index=self.rtm_newlink[ifname][0]['index'])
        self.ifnames = []
        # close the socket
        self.ip.close()

    def collect(self):
        '''
        Run the tests and collect the capabilities. They will be
        saved in the `TestCapsRtnl.capabilities` attribute.
        '''
        symbols = sorted(dir(self))
        # start the monitoring thread
        mthread = threading.Thread(target=self.monitor)
        mthread.start()
        self.event.wait()
        # wait for the thread setup
        for name in symbols:
            if name.startswith('test_'):
                self.setup()
                try:
                    ret = getattr(self, name)()
                    if ret is None:
                        ret = True
                    self.set_capability(name[5:], ret)
                except SkipTest:
                    self.set_capability(name[5:], None)
                except Exception:
                    for ifname in self.ifnames:
                        # cancel events queued for that test
                        self.rtm_events[ifname].set()
                    self.set_capability(name[5:], False)
                self.teardown()
        # stop the monitor
        os.write(self.cmdw, b'q')
        mthread.join()
        return self.capabilities

    def test_uname(self):
        '''
        Return collected uname
        '''
        return config.uname

    def test_python_version(self):
        '''
        Return Python version
        '''
        return sys.version

    def test_unpack_from(self):
        '''
        Does unpack_from() support bytearray as the buffer
        '''
        # probe unpack from
        try:
            struct.unpack_from('I', bytearray((1, 0, 0, 0)), 0)
        except:
            return False
        # works... but may it be monkey patched?
        if hasattr(struct, '_u_f_orig'):
            return False

    def test_create_dummy(self):
        '''
        An obvious test: an ability to create dummy interfaces
        '''
        self.ghost = self.ifname()
        self.ip.link('add', ifname=self.ghost, kind='dummy')

    def test_create_bridge(self):
        '''
        Can the kernel create bridges via netlink?
        '''
        self.ip.link('add', ifname=self.ifname(), kind='bridge')

    def test_create_bond(self):
        '''
        Can the kernel create bonds via netlink?
        '''
        self.ip.link('add', ifname=self.ifname(), kind='bond')

    def test_ghost_newlink_count(self):
        '''
        A normal flow (req == request, brd == broadcast message)::

            (req) -> RTM_NEWLINK
            (brd) <- RTM_NEWLINK
            (req) -> RTM_DELLINK
            (brd) <- RTM_DELLINK

        But on old kernels you can encounter the following::

            (req) -> RTM_NEWLINK
            (brd) <- RTM_NEWLINK
            (req) -> RTM_DELLINK
            (brd) <- RTM_DELLINK
            (brd) <- RTM_NEWLINK  (!) false positive

        And that obviously can break the code that relies on
        broadcast updates, since it will see as a new interface
        is created immediately after it was destroyed.

        One can ignore RTM_NEWLINK for the same name that follows
        a normal RTM_DELLINK. To do that, one should be sure the
        message will come.

        Another question is how many messages to ignore.

        This is not a test s.str., but it should follow after the
        `test_create_dummy`. It counts, how many RTM_NEWLINK
        messages arrived during the `test_create_dummy`.

        The ghost newlink messages count will be the same for other
        interface types as well.
        '''
        with open('/proc/version', 'r') as f:
            if int(f.read().split()[2][0]) > 2:
                # the issue is reported only for kernels 2.x
                return 0
        # there is no guarantee it will come; it *may* come
        self.rtm_events[self.ghost].wait(0.5)
        return max(len(self.rtm_newlink.get(self.ghost, [])) - 1, 0)
