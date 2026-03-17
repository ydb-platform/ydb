Introduction
============

About python-iptables
---------------------

**Iptables** is the tool that is used to manage **netfilter**, the
standard packet filtering and manipulation framework under Linux. As the
iptables manpage puts it:

> Iptables is used to set up, maintain, and inspect the tables of IPv4
> packet filter rules in the Linux kernel. Several different tables may
> be defined.
>
> Each table contains a number of built-in chains and may also contain
> user- defined chains.
>
> Each chain is a list of rules which can match a set of packets. Each
> rule specifies what to do with a packet that matches. This is called a
> target, which may be a jump to a user-defined chain in the same table.

`Python-iptables` provides a pythonesque wrapper via python bindings to
iptables under Linux. Interoperability with iptables is achieved via
using the iptables C libraries (`libiptc`, `libxtables`, and the
iptables extensions), not calling the iptables binary and parsing its
output. It is meant primarily for dynamic and/or complex routers and
firewalls, where rules are often updated or changed, or Python programs
wish to interface with the Linux iptables framework..

If you are looking for `ebtables` python bindings, check out
[python-ebtables](https://github.com/ldx/python-ebtables/).

`Python-iptables` supports Python 2.6, 2.7 and 3.4.

[![Flattr](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=ldx&url=https%3A%2F%2Fgithub.com%2Fldx%2Fpython-iptables)

[![Latest Release](https://img.shields.io/pypi/v/python-iptables.svg)](https://pypi.python.org/pypi/python-iptables)

[![Build Status](https://travis-ci.org/ldx/python-iptables.png?branch=master)](https://travis-ci.org/ldx/python-iptables)

[![Coverage Status](https://coveralls.io/repos/ldx/python-iptables/badge.svg?branch=codecoverage)](https://coveralls.io/r/ldx/python-iptables?branch=codecoverage)

[![Code Health](https://landscape.io/github/ldx/python-iptables/codecoverage/landscape.svg)](https://landscape.io/github/ldx/python-iptables/codecoverage)

[![Number of Downloads](https://img.shields.io/pypi/dm/python-iptables.svg)](https://pypi.python.org/pypi/python-iptables)

[![License](https://img.shields.io/pypi/l/python-iptables.svg)](https://pypi.python.org/pypi/python-iptables)

Installing via pip
------------------

The usual way:

    pip install --upgrade python-iptables

Compiling from source
---------------------

First make sure you have iptables installed (most Linux distributions
install it by default). `Python-iptables` needs the shared libraries
`libiptc.so` and `libxtables.so` coming with iptables, they are
installed in `/lib` on Ubuntu.

You can compile `python-iptables` in the usual distutils way:

    % cd python-iptables
    % python setup.py build

If you like, `python-iptables` can also be installed into a
`virtualenv`:

    % mkvirtualenv python-iptables
    % python setup.py install

If you install `python-iptables` as a system package, make sure the
directory where `distutils` installs shared libraries is in the dynamic
linker's search path (it's in `/etc/ld.so.conf` or in one of the files
in the folder `/etc/ld.co.conf.d`). Under Ubuntu `distutils` by default
installs into `/usr/local/lib`.

Now you can run the tests:

    % sudo PATH=$PATH python setup.py test
    WARNING: this test will manipulate iptables rules.
    Don't do this on a production machine.
    Would you like to continue? y/n y
    [...]

The `PATH=$PATH` part is necessary after `sudo` if you have installed
into a `virtualenv`, since `sudo` will reset your environment to a
system setting otherwise..

Once everything is in place you can fire up python to check whether the
package can be imported:

    % sudo PATH=$PATH python
    >>> import iptc
    >>>

Of course you need to be root to be able to use iptables.

Using a custom iptables install
-------------------------------

If you are stuck on a system with an old version of `iptables`, you can
install a more up to date version to a custom location, and ask
`python-iptables` to use libraries at that location.

To install `iptables` to `/tmp/iptables`:

    % git clone git://git.netfilter.org/iptables && cd iptables
    % ./autogen.sh
    % ./configure --prefix=/tmp/iptables
    % make
    % make install

Make sure the dependencies `iptables` needs are installed.

Now you can point `python-iptables` to this install path via:

    % sudo PATH=$PATH IPTABLES_LIBDIR=/tmp/iptables/lib XTABLES_LIBDIR=/tmp/iptables/lib/xtables python
    >>> import iptc
    >>>

What is supported
-----------------

The basic iptables framework and all the match/target extensions are
supported by `python-iptables`, including IPv4 and IPv6 ones. All IPv4
and IPv6 tables are supported as well.

Full documentation with API reference is available
[here](http://ldx.github.com/python-iptables/).

Examples
========

High level abstractions
-----------------------

``python-iptables`` implements a low-level interface that tries to closely
match the underlying C libraries. The module ``iptc.easy`` improves the
usability of the library by providing a rich set of high-level functions
designed to simplify the interaction with the library, for example:

    >>> import iptc
    >>> iptc.easy.dump_table('nat', ipv6=False)
    {'INPUT': [], 'OUTPUT': [], 'POSTROUTING': [], 'PREROUTING': []}
    >>> iptc.easy.dump_chain('filter', 'OUTPUT', ipv6=False)
    [{'comment': {'comment': 'DNS traffic to Google'},
      'counters': (1, 56),
      'dst': '8.8.8.8/32',
      'protocol': 'udp',
      'target': 'ACCEPT',
      'udp': {'dport': '53'}}]
    >>> iptc.easy.add_chain('filter', 'TestChain')
    True
    >>> rule_d = {'protocol': 'tcp', 'target': 'ACCEPT', 'tcp': {'dport': '22'}}
    >>> iptc.easy.insert_rule('filter', 'TestChain', rule_d)
    >>> iptc.easy.dump_chain('filter', 'TestChain')
    [{'protocol': 'tcp', 'target': 'ACCEPT', 'tcp': {'dport': '22'}}]
    >>> iptc.easy.delete_chain('filter', 'TestChain', flush=True)

    >>> # Example of goto rule // iptables -A FORWARD -p gre -g TestChainGoto
    >>> iptc.easy.add_chain('filter', 'TestChainGoto')
    >>> rule_goto_d = {'protocol': 'gre', 'target': {'goto': 'TestChainGoto'}}
    >>> iptc.easy.insert_rule('filter', 'FORWARD', rule_goto_d)

Rules
-----

In `python-iptables`, you usually first create a rule, and set any
source/destination address, in/out interface and protocol specifiers,
for example:

    >>> import iptc
    >>> rule = iptc.Rule()
    >>> rule.in_interface = "eth0"
    >>> rule.src = "192.168.1.0/255.255.255.0"
    >>> rule.protocol = "tcp"

This creates a rule that will match TCP packets coming in on eth0, with
a source IP address of 192.168.1.0/255.255.255.0.

A rule may contain matches and a target. A match is like a filter
matching certain packet attributes, while a target tells what to do with
the packet (drop it, accept it, transform it somehow, etc). One can
create a match or target via a Rule:

    >>> rule = iptc.Rule()
    >>> m = rule.create_match("tcp")
    >>> t = rule.create_target("DROP")

Match and target parameters can be changed after creating them. It is
also perfectly valid to create a match or target via instantiating them
with their constructor, but you still need a rule and you have to add
the matches and the target to their rule manually:

    >>> rule = iptc.Rule()
    >>> match = iptc.Match(rule, "tcp")
    >>> target = iptc.Target(rule, "DROP")
    >>> rule.add_match(match)
    >>> rule.target = target

Any parameters a match or target might take can be set via the
attributes of the object. To set the destination port for a TCP match:

    >>> rule = iptc.Rule()
    >>> rule.protocol = "tcp"
    >>> match = rule.create_match("tcp")
    >>> match.dport = "80"

To set up a rule that matches packets marked with 0xff:

    >>> rule = iptc.Rule()
    >>> rule.protocol = "tcp"
    >>> match = rule.create_match("mark")
    >>> match.mark = "0xff"

Parameters are always strings. You can supply any string as the
parameter value, but note that most extensions validate their
parameters. For example this:

    >>> rule = iptc.Rule()
    >>> rule.protocol = "tcp"
    >>> rule.target = iptc.Target(rule, "ACCEPT")
    >>> match = iptc.Match(rule, "state")
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> match.state = "RELATED,ESTABLISHED"
    >>> rule.add_match(match)
    >>> chain.insert_rule(rule)

will work. However, if you change the state parameter:

    >>> rule = iptc.Rule()
    >>> rule.protocol = "tcp"
    >>> rule.target = iptc.Target(rule, "ACCEPT")
    >>> match = iptc.Match(rule, "state")
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> match.state = "RELATED,ESTABLISHED,FOOBAR"
    >>> rule.add_match(match)
    >>> chain.insert_rule(rule)

`python-iptables` will throw an exception:

    Traceback (most recent call last):
      File "state.py", line 7, in <module>
        match.state = "RELATED,ESTABLISHED,FOOBAR"
      File "/home/user/Projects/python-iptables/iptc/ip4tc.py", line 369, in __setattr__
        self.parse(name.replace("_", "-"), value)
      File "/home/user/Projects/python-iptables/iptc/ip4tc.py", line 286, in parse
        self._parse(argv, inv, entry)
      File "/home/user/Projects/python-iptables/iptc/ip4tc.py", line 516, in _parse
        ct.cast(self._ptrptr, ct.POINTER(ct.c_void_p)))
      File "/home/user/Projects/python-iptables/iptc/xtables.py", line 736, in new
        ret = fn(*args)
      File "/home/user/Projects/python-iptables/iptc/xtables.py", line 1031, in parse_match
        argv[1]))
    iptc.xtables.XTablesError: state: parameter error -2 (RELATED,ESTABLISHED,FOOBAR)

Certain parameters take a string that optionally consists of multiple
words. The comment match is a good example:

    >>> rule = iptc.Rule()
    >>> rule.src = "127.0.0.1"
    >>> rule.protocol = "udp"
    >>> rule.target = rule.create_target("ACCEPT")
    >>> match = rule.create_match("comment")
    >>> match.comment = "this is a test comment"
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> chain.insert_rule(rule)

Note that this is still just one parameter value.

However, when a match or a target takes multiple parameter values, that
needs to be passed in as a list. Let's assume you have created and set
up an `ipset` called `blacklist` via the `ipset` command. To create a
rule with a match for this set:

    >>> rule = iptc.Rule()
    >>> m = rule.create_match("set")
    >>> m.match_set = ['blacklist', 'src']

Note how this time a list was used for the parameter value, since the
`set` match `match_set` parameter expects two values. See the `iptables`
manpages to find out what the extensions you use expect. See
[ipset](http://ipset.netfilter.org/) for more information.

When you are ready constructing your rule, add them to the chain you
want it to show up in:

    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> chain.insert_rule(rule)

This will put your rule into the INPUT chain in the filter table.

Chains and tables
-----------------

You can of course also check what a rule's source/destination address,
in/out inteface etc is. To print out all rules in the FILTER table:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> for chain in table.chains:
    >>>     print "======================="
    >>>     print "Chain ", chain.name
    >>>     for rule in chain.rules:
    >>>         print "Rule", "proto:", rule.protocol, "src:", rule.src, "dst:", \
    >>>               rule.dst, "in:", rule.in_interface, "out:", rule.out_interface,
    >>>         print "Matches:",
    >>>         for match in rule.matches:
    >>>             print match.name,
    >>>         print "Target:",
    >>>         print rule.target.name
    >>> print "======================="

As you see in the code snippet above, rules are organized into chains,
and chains are in tables. You have a fixed set of tables; for IPv4:

-   `FILTER`,
-   `NAT`,
-   `MANGLE` and
-   `RAW`.

For IPv6 the tables are:

-   `FILTER`,
-   `MANGLE`,
-   `RAW` and
-   `SECURITY`.

To access a table:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> print table.name
    filter

To create a new chain in the FILTER table:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = table.create_chain("testchain")

    $ sudo iptables -L -n
    [...]
    Chain testchain (0 references)
    target     prot opt source               destination

To access an existing chain:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = iptc.Chain(table, "INPUT")
    >>> chain.name
    'INPUT'
    >>> len(chain.rules)
    10
    >>>

More about matches and targets
------------------------------

There are basic targets, such as `DROP` and `ACCEPT`. E.g. to reject
packets with source address `127.0.0.1/255.0.0.0` coming in on any of
the `eth` interfaces:

    >>> import iptc
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> rule = iptc.Rule()
    >>> rule.in_interface = "eth+"
    >>> rule.src = "127.0.0.1/255.0.0.0"
    >>> target = iptc.Target(rule, "DROP")
    >>> rule.target = target
    >>> chain.insert_rule(rule)

To instantiate a target or match, we can either create an object like
above, or use the `rule.create_target(target_name)` and
`rule.create_match(match_name)` methods. For example, in the code above
target could have been created as:

    >>> target = rule.create_target("DROP")

instead of:

    >>> target = iptc.Target(rule, "DROP")
    >>> rule.target = target

The former also adds the match or target to the rule, saving a call.

Another example, using a target which takes parameters. Let's mark
packets going to `192.168.1.2` UDP port `1234` with `0xffff`:

    >>> import iptc
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.MANGLE), "PREROUTING")
    >>> rule = iptc.Rule()
    >>> rule.dst = "192.168.1.2"
    >>> rule.protocol = "udp"
    >>> match = iptc.Match(rule, "udp")
    >>> match.dport = "1234"
    >>> rule.add_match(match)
    >>> target = iptc.Target(rule, "MARK")
    >>> target.set_mark = "0xffff"
    >>> rule.target = target
    >>> chain.insert_rule(rule)

Matches are optional (specifying a target is mandatory). E.g. to insert
a rule to NAT TCP packets going out via `eth0`:

    >>> import iptc
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.NAT), "POSTROUTING")
    >>> rule = iptc.Rule()
    >>> rule.protocol = "tcp"
    >>> rule.out_interface = "eth0"
    >>> target = iptc.Target(rule, "MASQUERADE")
    >>> target.to_ports = "1234"
    >>> rule.target = target
    >>> chain.insert_rule(rule)

Here only the properties of the rule decide whether the rule will be
applied to a packet.

Matches are optional, but we can add multiple matches to a rule. In the
following example we will do that, using the `iprange` and the `tcp`
matches:

    >>> import iptc
    >>> rule = iptc.Rule()
    >>> rule.protocol = "tcp"
    >>> match = iptc.Match(rule, "tcp")
    >>> match.dport = "22"
    >>> rule.add_match(match)
    >>> match = iptc.Match(rule, "iprange")
    >>> match.src_range = "192.168.1.100-192.168.1.200"
    >>> match.dst_range = "172.22.33.106"
    >>> rule.add_match(match)
    >>> rule.target = iptc.Target(rule, "DROP")
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> chain.insert_rule(rule)

This is the `python-iptables` equivalent of the following iptables
command:

    # iptables -A INPUT -p tcp –destination-port 22 -m iprange –src-range 192.168.1.100-192.168.1.200 –dst-range 172.22.33.106 -j DROP

You can of course negate matches, just like when you use `!` in front of
a match with iptables. For example:

    >>> import iptc
    >>> rule = iptc.Rule()
    >>> match = iptc.Match(rule, "mac")
    >>> match.mac_source = "!00:11:22:33:44:55"
    >>> rule.add_match(match)
    >>> rule.target = iptc.Target(rule, "ACCEPT")
    >>> chain = iptc.Chain(iptc.Table(iptc.Table.FILTER), "INPUT")
    >>> chain.insert_rule(rule)

This results in:

    $ sudo iptables -L -n
    Chain INPUT (policy ACCEPT)
    target     prot opt source               destination
    ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0            MAC ! 00:11:22:33:44:55

    Chain FORWARD (policy ACCEPT)
    target     prot opt source               destination

    Chain OUTPUT (policy ACCEPT)
    target     prot opt source               destination

Counters
--------

You can query rule and chain counters, e.g.:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = iptc.Chain(table, 'OUTPUT')
    >>> for rule in chain.rules:
    >>>         (packets, bytes) = rule.get_counters()
    >>>         print packets, bytes

However, the counters are only refreshed when the underlying low-level
iptables connection is refreshed in `Table` via `table.refresh()`. For
example:

    >>> import time, sys
    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = iptc.Chain(table, 'OUTPUT')
    >>> for rule in chain.rules:
    >>>         (packets, bytes) = rule.get_counters()
    >>>         print packets, bytes
    >>> print "Please send some traffic"
    >>> sys.stdout.flush()
    >>> time.sleep(3)
    >>> for rule in chain.rules:
    >>>         # Here you will get back the same counter values as above
    >>>         (packets, bytes) = rule.get_counters()
    >>>         print packets, bytes

This will show you the same counter values even if there was traffic
hitting your rules. You have to refresh your table to get update your
counters:

    >>> import time, sys
    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = iptc.Chain(table, 'OUTPUT')
    >>> for rule in chain.rules:
    >>>         (packets, bytes) = rule.get_counters()
    >>>         print packets, bytes
    >>> print "Please send some traffic"
    >>> sys.stdout.flush()
    >>> time.sleep(3)
    >>> table.refresh()  # Here: refresh table to update rule counters
    >>> for rule in chain.rules:
    >>>         (packets, bytes) = rule.get_counters()
    >>>         print packets, bytes

What is more, if you add:

    iptables -A OUTPUT -p tcp --sport 80
    iptables -A OUTPUT -p tcp --sport 22

you can query rule and chain counters together with the protocol and
sport(or dport), e.g.:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = iptc.Chain(table, 'OUTPUT')
    >>> for rule in chain.rules:
    >>>         for match in rule.matches:
    >>>             (packets, bytes) = rule.get_counters()
    >>>             print packets, bytes, match.name, match.sport

Autocommit
----------

`Python-iptables` by default automatically performs an iptables commit
after each operation. That is, after you add a rule in
`python-iptables`, that will take effect immediately.

It may happen that you want to batch together certain operations. A
typical use case is traversing a chain and removing rules matching a
specific criteria. If you do this with autocommit enabled, after the
first delete operation, your chain's state will change and you have to
restart the traversal. You can do something like this:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> removed = True
    >>> chain = iptc.Chain(table, "FORWARD")
    >>> while removed == True:
    >>>     removed = False
    >>>     for rule in chain.rules:
    >>>         if rule.out_interface and "eth0" in rule.out_interface:
    >>>             chain.delete_rule(rule)
    >>>             removed = True
    >>>             break

This is clearly not ideal and the code is not very readable. An
alternative is to disable autocommits, traverse the chain, removing one
or more rules, than commit it:

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> table.autocommit = False
    >>> chain = iptc.Chain(table, "FORWARD")
    >>> for rule in chain.rules:
    >>>     if rule.out_interface and "eth0" in rule.out_interface:
    >>>         chain.delete_rule(rule)
    >>> table.commit()
    >>> table.autocommit = True

The drawback is that Table is a singleton, and if you disable
autocommit, it will be disabled for all instances of that Table.

Easy rules with dictionaries
----------------------------
To simplify operations with ``python-iptables`` rules we have included support to define and convert Rules object into python dictionaries.

    >>> import iptc
    >>> table = iptc.Table(iptc.Table.FILTER)
    >>> chain = iptc.Chain(table, "INPUT")
    >>> # Create an iptc.Rule object from dictionary
    >>> rule_d = {'comment': {'comment': 'Match tcp.22'}, 'protocol': 'tcp', 'target': 'ACCEPT', 'tcp': {'dport': '22'}}
    >>> rule = iptc.easy.encode_iptc_rule(rule_d)
    >>> # Obtain a dictionary representation from the iptc.Rule
    >>> iptc.easy.decode_iptc_rule(rule)
    {'tcp': {'dport': '22'}, 'protocol': 'tcp', 'comment': {'comment': 'Match tcp.22'}, 'target': 'ACCEPT'}


Known Issues
============

These issues are mainly caused by complex interaction with upstream's
Netfilter implementation, and will require quite significant effort to
fix. Workarounds are available.

-   The `hashlimit` match requires explicitly setting
    `hashlimit_htable_expire`. See [Issue
    \#201](https://github.com/ldx/python-iptables/issues/201).
-   The `NOTRACK` target is problematic; use `CT --notrack` instead. See
    [Issue \#204](https://github.com/ldx/python-iptables/issues/204).

