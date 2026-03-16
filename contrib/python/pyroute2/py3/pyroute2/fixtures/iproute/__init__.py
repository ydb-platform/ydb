import errno
from collections.abc import AsyncGenerator, Generator
from typing import Generic, TypeVar

import pytest
import pytest_asyncio

from pyroute2 import netns
from pyroute2.common import uifname
from pyroute2.iproute.linux import AsyncIPRoute, IPRoute
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.netlink.rtnl import IFNAMSIZ
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg

T = TypeVar('T', AsyncIPRoute, IPRoute)


class TestInterface:
    '''Test interface spec.

    Provided by `test_link` fixture.

    Provides shortcuts to some important interface properties,
    like `TestInterface.index` or `TestInterface.netns`.
    '''

    def __init__(self, index: int, ifname: str, address: str, nsname: str):
        if index <= 1:
            raise TypeError('test interface index must be > 1')
        if not 1 < len(ifname) <= IFNAMSIZ:
            raise TypeError(
                'test interface ifname length must be from 2 to IFNAMSIZ'
            )
        self._index = index
        self._ifname = ifname
        self._address = address
        self._netns = nsname

    @property
    def index(self) -> int:
        '''Test interface index.

        Is always greater than 1, as index 1 has the the loopback interface.
        '''
        return self._index

    @property
    def ifname(self) -> str:
        '''Test interface ifname.

        The name length must be greater than 1 and less or equal IFNAMSIZ.
        '''
        return self._ifname

    @property
    def address(self) -> str:
        '''Test interface MAC address.

        In the form `xx:xx:xx:xx:xx:xx`.'''
        return self._address

    @property
    def netns(self) -> str:
        '''Test interface netns.

        A string name of the network namespace.'''
        return self._netns


class TestContext(Generic[T]):
    '''The test context.

    Provided by `async_context` and `sync_context` fixtures.

    Provides convenient shortcuts to RTNL API, the network namespace
    name and the test interface spec.
    '''

    __test__ = False  # prevent pytest from looking for tests here

    def __init__(self, ipr: T, test_link: TestInterface):
        self._ipr: T = ipr
        self._test_link = test_link

    @property
    def ipr(self) -> T:
        '''RTNL API.

        Return RTNL API instance, either IPRoute, or AsyncIPRoute.'''
        return self._ipr

    @property
    def test_link(self) -> TestInterface:
        '''Test interface spec.

        Return `TestInterface` object for the test interface.'''
        return self._test_link

    @property
    def netns(self) -> str:
        '''Network namespace.

        A string name of the network namespace.'''
        return self.ipr.status['netns']


class SetNSContext:
    '''A unique network namespace context.

    Provided by `setns_context` fixture.

    Sets a unique netns for the whole python process for the
    fixture scope, and returns from the nets on the scope exit.'''

    def __init__(self, nsname: str):
        self._netns = nsname

    def __enter__(self):
        netns.pushns(self.netns)

    def __exit__(self, *_):
        netns.popns()

    @property
    def netns(self) -> str:
        '''Network namespace.

        A string name of the network namespace.'''
        return self._netns


@pytest.fixture
def nsname() -> Generator[str]:
    '''Network namespace.

    * **Name**: nsname
    * **Scope**: function

    Create a unique network namespace and yield its name. Remove
    the netns on cleanup.

    It's safe to create and modify interfaces, addresses, routes etc.
    in the test network namespace, as it is disconnected from the main
    system, and the test cleanup will remove the namespace with all
    its content.

    Example usage:

    .. testcode:: nsname

        def test_list_interfaces(nsname):
            subprocess.Popen(
                ['ip', 'netns', 'exec', nsname, 'ip', 'link'],
                stdout=subprocess.PIPE,
            )
            # ...
    '''
    nsname = uifname()
    netns.create(nsname)
    with IPRoute(netns=nsname) as ipr:
        ipr.link('set', index=1, state='up')
        ipr.poll(ipr.addr, 'dump', address='127.0.0.1', timeout=5)
    yield nsname
    try:
        netns.remove(nsname)
    except OSError:
        pass


@pytest.fixture
def test_link_ifinfmsg(nsname: str) -> Generator[ifinfmsg]:
    '''Test interface ifinfmsg.

    * **Name**: test_link_ifinfmsg
    * **Scope**: function
    * **Depends**: nsname

    Create a test interface in the test netns and yield ifinfmsg. Remove
    the interface on cleanup.

    This fixture depends on **nsname**, and it means that the network
    namespace will be created automatically if you use this fixture.

    Example usage:

    .. testcode:: test_link_ifinfmsg

        def test_check_interface(nsname, test_link_ifinfmsg):
            link = test_link_ifinfmsg
            ns = ['ip', 'netns', 'exec', nsname]
            up = ['ip', 'link', 'set', 'dev', link.get('ifname'), 'up']
            subprocess.Popen(ns + up)
            # ...

    '''
    ifname = uifname()
    with IPRoute(netns=nsname) as ipr:
        ipr.link('add', ifname=ifname, kind='dummy', state='up')
        (link,) = ipr.poll(ipr.link, 'dump', ifname=ifname, timeout=5)
        yield link
        try:
            ipr.link('del', index=link.get('index'))
        except NetlinkError as e:
            if e.code != errno.ENODEV:
                raise


@pytest.fixture
def test_link(
    nsname: str, test_link_ifinfmsg: ifinfmsg
) -> Generator[TestInterface]:
    '''Test interface spec.

    * **Name**: test_link
    * **Scope**: function
    * **Depends**: nsname, test_link_ifinfmsg

    Yield `TestInterface` object for the test interface, providing
    a convenient way to access some important interface properties.
    '''
    yield TestInterface(
        index=test_link_ifinfmsg.get('index'),
        ifname=test_link_ifinfmsg.get('ifname'),
        address=test_link_ifinfmsg.get('address'),
        nsname=nsname,
    )


@pytest.fixture
def test_link_address(test_link: TestInterface) -> Generator[str]:
    '''Test interface MAC address.

    * **Name**: test_link_address
    * **Scope**: function
    * **Depends**: test_link

    Yield test interface MAC address. The network namespace and
    the test interface exist at this point.
    '''
    yield test_link.address


@pytest.fixture
def test_link_index(test_link: TestInterface) -> Generator[int]:
    '''Test interface MAC index.

    * **Name**: test_link_index
    * **Scope**: function
    * **Depends**: test_link

    Yield test interface index. The network namespace and
    the test interface exist at this point.
    '''
    yield test_link.index


@pytest.fixture
def test_link_ifname(test_link: TestInterface) -> Generator[str]:
    '''Test interface MAC ifname.

    * **Name**: test_link_ifname
    * **Scope**: function
    * **Depends**: test_link

    Yield test interface ifname. The network namespace and
    the test interface exist at this point.
    '''
    yield test_link.ifname


@pytest.fixture
def tmp_link_ifname(nsname: str) -> Generator[str]:
    '''Temporary link name.

    * **Name**: tmp_link_ifname
    * **Scope**: function
    * **Depends**: nsname

    Yield tmp link ifname, but don't create it. Try to remove
    the link on cleanup.
    '''
    ifname = uifname()
    with IPRoute(netns=nsname) as ipr:
        yield ifname
        try:
            (link,) = ipr.link('get', ifname=ifname)
            ipr.link('del', index=link.get('index'))
        except NetlinkError as e:
            if e.code != errno.ENODEV:
                raise


@pytest_asyncio.fixture
async def async_ipr(request, nsname: str) -> AsyncGenerator[AsyncIPRoute]:
    '''`AsyncIPRoute` instance.

    * **Name**: async_ipr
    * **Scope**: function
    * **Depends**: nsname

    Yield `AsyncIPRoute` instance, running within the test network namespace.
    You can provide additional keyword arguments to `AsyncIPRoute`:

    .. testcode:: async_ipr

        @pytest.mark.parametrize(
            'async_ipr',
            [
                {
                    'ext_ack': True,
                    'strict_check': True,
                },
            ],
            indirect=True
        )
        @pytest.mark.asyncio
        async def test_my_case(async_ipr):
            await async_ipr.link('set', index=1, state='up')

    '''
    kwarg = getattr(request, 'param', {})
    async with AsyncIPRoute(netns=nsname, **kwarg) as ipr:
        yield ipr


@pytest.fixture
def sync_ipr(request, nsname: str) -> Generator[IPRoute]:
    '''`IPRoute` instance.

    * **Name**: sync_ipr
    * **Scope**: function
    * **Depends**: nsname

    Yield `IPRoute` instance, running within the test network namespace.
    You can provide additional keyword arguments to `IPRoute`:

    .. testcode:: sync_ipr

        @pytest.mark.parametrize(
            'sync_ipr',
            [
                {
                    'ext_ack': True,
                    'strict_check': True,
                },
            ],
            indirect=True
        )
        def test_my_case(sync_ipr):
            sync_ipr.link('set', index=1, state='up')
    '''
    kwarg = getattr(request, 'param', {})
    with IPRoute(netns=nsname, **kwarg) as ipr:
        yield ipr


@pytest_asyncio.fixture
async def async_context(
    async_ipr: AsyncIPRoute, test_link: TestInterface
) -> AsyncGenerator[TestContext[AsyncIPRoute]]:
    '''Asynchronous TestContext.

    * **Name**: async_context
    * **Scope**: function
    * **Depends**: async_ipr, test_link

    Yield `TestContext` with `AsyncIPRoute`.
    '''
    yield TestContext[AsyncIPRoute](async_ipr, test_link)


@pytest.fixture
def sync_context(
    sync_ipr: IPRoute, test_link: TestInterface
) -> Generator[TestContext[IPRoute]]:
    '''Synchronous TestContext.

    * **Name**: sync_context
    * **Scope**: function
    * **Depends**: sync_ipr, test_link

    Yield `TestContext` with `IPRoute`.
    '''
    yield TestContext[IPRoute](sync_ipr, test_link)


@pytest.fixture
def setns_context(nsname: str) -> Generator[SetNSContext]:
    '''Set network namespace.

    * **Name**: setns_context
    * **Scope**: function
    * **Depends**: nsname

    Set a unique network namespace for the current process. Push the new
    netns onto the stack, yield the network namespace context, and pop and
    cleanup netns on exit.

    Please notice that `setns()` call affects the whole python process.
    '''
    with SetNSContext(nsname) as ctx:
        yield ctx
