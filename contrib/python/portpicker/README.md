# Python portpicker module

[![PyPI version](https://badge.fury.io/py/portpicker.svg)](https://badge.fury.io/py/portpicker)
[![GH Action Status](https://github.com/google/python_portpicker/actions/workflows/python-package.yml/badge.svg)](https://github.com/google/python_portpicker/actions)

This module is useful for finding unused network ports on a host. If you need
legacy Python 2 support, use the 1.3.x releases.

This module provides a pure Python `pick_unused_port()` function. It can also be
called via the command line for use in shell scripts.

If your code can accept a bound TCP socket rather than a port number consider
using `socket.bind(('localhost', 0))` to bind atomically to an available port
rather than using this library at all.

There is a race condition between picking a port and your application code
binding to it. The use of a port server by all of your test code to avoid that
problem is recommended on loaded test hosts running many tests at a time.

Unless you are using a port server, subsequent calls to `pick_unused_port()` to
obtain an additional port are not guaranteed to return a unique port.

### What is the optional port server?

A port server is intended to be run as a daemon, for use by all processes
running on the host. It coordinates uses of network ports by anything using a
portpicker library. If you are using hosts as part of a test automation cluster,
each one should run a port server as a daemon. You should set the
`PORTSERVER_ADDRESS=@unittest-portserver` environment variable on all of your
test runners so that portpicker makes use of it.

A sample port server is included. This portserver implementation works but has
not spent time in production. If you use it with good results please report back
so that this statement can be updated to reflect that. :)

A port server listens on a unix socket, reads a pid from a new connection, tests
the ports it is managing and replies with a port assignment port for that pid. A
port is only reclaimed for potential reassignment to another process after the
process it was originally assigned to has died. Processes that need multiple
ports can simply issue multiple requests and are guaranteed they will each be
unique.

## Typical usage:

```python
import portpicker
test_port = portpicker.pick_unused_port()
```

Or from the command line:

```bash
TEST_PORT=`/path/to/portpicker.py $$`
```

Or, if portpicker is installed as a library on the system Python interpreter:

```bash
TEST_PORT=`python3 -m portpicker $$`
```

## DISCLAIMER

This is not an official Google product (experimental or otherwise), it is just
code that happens to be owned by Google.
