.. warning:: Please read about the coming changes in the packaging: https://github.com/svinota/pyroute2/discussions/786

Pyroute2
========

Pyroute2 is a pure Python **netlink** library. The core requires only Python
stdlib, no 3rd party libraries. The library was started as an RTNL protocol
implementation, so the name is **pyroute2**, but now it supports many netlink
protocols. Some supported netlink families and protocols:

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

    ndb = NDB(log='debug')

    for record in ndb.interfaces.summary():
        print(record.ifname, record.address, record.state)

    print(ndb
          .interfaces
          .dump()
          .select('index', 'ifname', 'kind')
          .format('json'))

    print(ndb
          .addresses
          .summary()
          .format('csv'))

    (ndb
     .interfaces
     .create(ifname='br0', kind='bridge')  # create a bridge
     .add_port('eth0')                     # add ports
     .add_port('eth1')                     #
     .add_ip('10.0.0.1/24')                # add addresses
     .add_ip('192.168.0.1/24')             #
     .set('br_stp_state', 1)               # set STP on
     .set('br_group_fwd_mask', 0x4000)     # set LLDP forwarding
     .set('state', 'up')                   # bring the interface up
     .commit())                            # commit pending changes

    # operate on netns:
    ndb.sources.add(netns='testns')                 # connect to a namespace
    
    (ndb.interfaces.create(
        **{'ifname': 'veth0',                       # create veth
           'kind': 'veth',                          #
           'peer': {'ifname': 'eth0',               # setup peer
                    'net_ns_fd': 'testns'}})        # in the namespace
     .set('state', 'up')                            #
     .add_ip(address='172.16.230.1', prefixlen=24)  # add address
     .commit())

    (ndb
     .interfaces
     .wait(**{'target': 'testns', 'ifname': 'eth0'}) # wait for the peer
     .set('state', 'up')                             # bring it up
     .add_ip(address='172.16.230.2', prefixlen=24)   # add address
     .commit())

IPRoute -- Low level RTNL API
-----------------------------

Low-level **IPRoute** utility --- Linux network configuration.
The **IPRoute** class is a 1-to-1 RTNL mapping. There are no implicit
interface lookups and so on.

Get notifications about network settings changes with IPRoute:

.. code-block:: python

    from pyroute2 import IPRoute
    with IPRoute() as ipr:
        # With IPRoute objects you have to call bind() manually
        ipr.bind()
        for message in ipr.get():
            print(message)

More examples:

.. code-block:: python

    from socket import AF_INET
    from pyroute2 import IPRoute

    # get access to the netlink socket
    ip = IPRoute()
    # no monitoring here -- thus no bind()

    # print interfaces
    for link in ip.get_links():
        print(link)

    # create VETH pair and move v0p1 to netns 'test'
    ip.link('add', ifname='v0p0', peer='v0p1', kind='veth')
    idx = ip.link_lookup(ifname='v0p1')[0]
    ip.link('set', index=idx, net_ns_fd='test')

    # bring v0p0 up and add an address
    idx = ip.link_lookup(ifname='v0p0')[0]
    ip.link('set', index=idx, state='up')
    ip.addr('add', index=idx, address='10.0.0.1', prefixlen=24)

    # release Netlink socket
    ip.close()

Network namespace examples
--------------------------

Network namespace manipulation:

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

        # lookup the peer index
        idx = ipr.link_lookup(ifname='v0p1')[0]

        # move the peer to the 'test' netns:
        ipr.link('set', index='v0p1', net_ns_fd='test')

List interfaces in some **netns**:

.. code-block:: python

    from pyroute2 import NetNS
    from pprint import pprint

    ns = NetNS('test')
    pprint(ns.get_links())
    ns.close()

More details and samples see in the documentation.

Installation
------------

`make install` or `pip install pyroute2`

Requirements
------------

Python >= 3.6

Python 2.7 or above also may work, but neither supported nor tested anymore.

The pyroute2 testing and documentaion framework requirements:

* flake8
* coverage
* nosetests
* pytest
* sphinx
* aafigure
* netaddr
* dtcd (optional, https://github.com/svinota/dtcd)

Optional dependencies:

* mitogen -- for distributed rtnl
* psutil -- for ss2 tool

Links
-----

* home: https://pyroute2.org/
* srcs: https://github.com/svinota/pyroute2
* bugs: https://github.com/svinota/pyroute2/issues
* pypi: https://pypi.python.org/pypi/pyroute2
* docs: http://docs.pyroute2.org/
