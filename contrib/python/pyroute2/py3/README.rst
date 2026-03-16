Pyroute2
========

Pyroute2 is a pure Python networking framework. The core requires only Python
stdlib, no 3rd party libraries. The library was started as an RTNL protocol
implementation, so the name is **pyroute2**, but now it supports several
protocols, including non-netlink. Here are some supported netlink families
and protocols:

* **dhcp** --- dynamic host configuration protocol for IPv4
* **9p2000** --- Plan9 file system protocol

Netlink:

* **rtnl**, network settings --- addresses, routes, traffic controls
* **nfnetlink** --- netfilter API
* **ipq** --- simplest userspace packet filtering, iptables QUEUE target
* **devlink** --- manage and monitor devlink-enabled hardware
* **generic** --- generic netlink families
* **uevent** --- same uevent messages as in udev

Netfilter API:

* **ipset** --- IP sets
* **nftables** --- packet filtering
* **nfct** --- connection tracking

Generic netlink:

* **ethtool** --- low-level network interface setup
* **wireguard** --- VPN setup
* **nl80211** --- wireless functions API (basic support)
* **taskstats** --- extended process statistics
* **acpi_events** --- ACPI events monitoring
* **thermal_events** --- thermal events monitoring
* **VFS_DQUOT** --- disk quota events monitoring

On the low level the library provides socket objects with an
extended API. The additional functionality aims to:

* Help to open/bind netlink sockets
* Discover generic netlink protocols and multicast groups
* Construct, encode and decode netlink and PF_ROUTE messages

Supported systems
-----------------

Pyroute2 runs natively on Linux and emulates some limited subset
of RTNL netlink API on BSD systems on top of PF_ROUTE notifications
and standard system tools.

Other platforms are not supported.

IPRoute -- synchronous RTNL API
-------------------------------

Low-level **IPRoute** utility --- Linux network configuration, this
class is almost a 1-to-1 RTNL mapping. There are no implicit
interface lookups and so on.

Get notifications about network settings changes:

.. code-block:: python

    from pyroute2 import IPRoute

    with IPRoute() as ipr:
        ipr.bind()  # <--- start listening for RTNL broadcasts
        for message in ipr.get():  # receive the broadcasts
            print(message)

More examples:

.. code-block:: python

    from socket import AF_INET
    from pyroute2 import IPRoute

    # get access to the netlink socket
    ipr = IPRoute()
    # no monitoring here -- thus no bind()

    # print interfaces
    for link in ipr.get_links():
        print(link)

    # create VETH pair and move v0p1 to netns 'test'
    ipr.link('add', ifname='v0p0', peer='v0p1', kind='veth')
    # wait for the devices:
    peer, veth = ipr.poll(
        ipr.link, 'dump', timeout=5, ifname=lambda x: x in ('v0p0', 'v0p1')
    )
    ipr.link('set', index=peer['index'], net_ns_fd='test')

    # bring v0p0 up and add an address
    ipr.link('set', index=veth['index'], state='up')
    ipr.addr('add', index=veth['index'], address='10.0.0.1', prefixlen=24)

    # release Netlink socket
    ip.close()

AsyncIPRoute -- asynchronous RTNL API
-------------------------------------

While `IPRoute` provides a synchronous RTNL API, it is actually build
around the asyncio-based core.

The same example as above can look like that:

.. code-block:: python

    import asyncio

    from pyroute2 import AsyncIPRoute

    async def main():
        # get access to the netlink socket
        ipr = AsyncIPRoute()

        # print interfaces
        async for link in await ipr.get_links():
            print(link)

        # create VETH pair and move v0p1 to netns 'test'
        await ipr.link('add', ifname='v0p0', peer='v0p1', kind='veth')

        # wait for the devices:
        peer, veth = await ipr.poll(
            ipr.link, 'dump', timeout=5, ifname=lambda x: x in ('v0p0', 'v0p1')
        )
        await ipr.link('set', index=peer['index'], net_ns_fd='test')
        ...
        ipr.close()

     asyncio.run(main())

Please notice that `.close()` is synchronous in any case.

Network namespace management
----------------------------

.. code-block:: python

    from pyroute2 import netns
    # create netns
    netns.create('test')
    # list
    print(netns.listnetns())
    # remove netns
    netns.remove('test')

Create **veth** interfaces pair and move to **netns**:

.. code-block:: python

    from pyroute2 import IPRoute

    with IPRoute() as ipr:

        # create interface pair
        ipr.link('add', ifname='v0p0', kind='veth',  peer='v0p1')

        # wait for the peer
        (peer,) = ipr.poll(ipr.link, 'dump', timeout=5, ifname='v0p1')

        # move the peer to the 'test' netns:
        ipr.link('set', index=peer['index'], net_ns_fd='test')

List interfaces in some **netns**:

.. code-block:: python

    from pyroute2 import IPRoute

    with IPRoute(netns='test') as ipr:
        for link in ipr.get_links():
            print(link)

More details and samples see in the documentation.

NDB -- high level RTNL API
--------------------------

Key features:

* Data integrity
* Transactions with commit/rollback changes
* State synchronization
* Multiple sources, including netns and remote systems

A "Hello world" example:

.. code-block:: python

    from pyroute2 import NDB

    with NDB() as ndb:
        with ndb.interfaces['eth0'] as eth0:
            # set one parameter
            eth0.set(state='down')
            eth0.commit()  # make sure that the interface is down
            # or multiple parameters at once
            eth0.set(ifname='hello_world!', state='up')
            eth0.commit()  # rename, bring up and wait for success
        # --> <-- here you can be sure that the interface is up & renamed

Installation
------------

Using pypi:

.. code-block:: bash

    pip install pyroute2

Using git:

.. code-block:: bash

    pip install git+https://github.com/svinota/pyroute2.git

Using source, requires make and nox

.. code-block:: bash

    git clone https://github.com/svinota/pyroute2.git
    cd pyroute2
    make install

Requirements
------------

Python >= 3.9

Links
-----

* home: https://pyroute2.org/
* source: https://github.com/svinota/pyroute2
* bugs: https://github.com/svinota/pyroute2/issues
* pypi: https://pypi.python.org/pypi/pyroute2
* docs: http://docs.pyroute2.org/
