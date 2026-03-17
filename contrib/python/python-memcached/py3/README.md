## Overview

This software is a 100% Python interface to the memcached memory cache
daemon.  It is the client side software which allows storing values
in one or more, possibly remote, memcached servers.  Search google for
memcached for more information.

This library is stable and largely in maintenance mode.  Another library that
is getting more active enhancements is
[pymemcache](https://pypi.org/project/pymemcache/) and they have links and a
good set of comparisons between them on their page.

This package was originally written by Evan Martin of Danga.  Please do
not contact Evan about maintenance.  Sean Reifschneider of tummy.com,
ltd. has taken over maintenance of it.

Please report issues and submit code changes to the github repository at:

   https://github.com/linsomniac/python-memcached

For changes prior to 2013-03-26, see the old Launchpad repository at:

   Historic issues: https://launchpad.net/python-memcached

## Testing

Test patches locally and easily by running tox:

    pip install tox
    tox -e py312

Test for style by running tox:

    tox -e pep8
