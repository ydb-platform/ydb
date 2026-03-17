tzlocal
=======

This Python module returns a ``tzinfo`` object with the local timezone information under Unix and Win-32.
It requires ``pytz``, and returns ``pytz`` ``tzinfo`` objects.

This module attempts to fix a glaring hole in ``pytz``, that there is no way to
get the local timezone information, unless you know the zoneinfo name, and
under several Linux distros that's hard or impossible to figure out.

Also, with Windows different timezone system using pytz isn't of much use
unless you separately configure the zoneinfo timezone name.

With ``tzlocal`` you only need to call ``get_localzone()`` and you will get a
``tzinfo`` object with the local time zone info. On some Unices you will still
not get to know what the timezone name is, but you don't need that when you
have the tzinfo file. However, if the timezone name is readily available it
will be used.


Supported systems
-----------------

These are the systems that are in theory supported:

 * Windows 2000 and later

 * Any unix-like system with a ``/etc/localtime`` or ``/usr/local/etc/localtime``

If you have one of the above systems and it does not work, it's a bug.
Please report it.

Please note that if you getting a time zone called ``local``, this is not a bug, it's
actually the main feature of ``tzlocal``, that even if your system does NOT have a configuration file
with the zoneinfo name of your time zone, it will still work.

You can also use ``tzlocal`` to get the name of your local timezone, but only if your system is
configured to make that possible. ``tzlocal`` looks for the timezone name in ``/etc/timezone``, ``/var/db/zoneinfo``,
``/etc/sysconfig/clock`` and ``/etc/conf.d/clock``. If your ``/etc/localtime`` is a symlink it can also extract the
name from that symlink.

If you need the name of your local time zone, then please make sure your system is properly configured to allow that.
If it isn't configured, tzlocal will default to UTC.

Usage
-----

Load the local timezone:

    >>> from tzlocal import get_localzone
    >>> tz = get_localzone()
    >>> tz
    <DstTzInfo 'Europe/Warsaw' WMT+1:24:00 STD>

Create a local datetime:

    >>> from datetime import datetime
    >>> dt = tz.localize(datetime(2015, 4, 10, 7, 22))
    >>> dt
    datetime.datetime(2015, 4, 10, 7, 22, tzinfo=<DstTzInfo 'Europe/Warsaw' CEST+2:00:00 DST>)

Lookup another timezone with `pytz`:

    >>> import pytz
    >>> eastern = pytz.timezone('US/Eastern')

Convert the datetime:

    >>> dt.astimezone(eastern)
    datetime.datetime(2015, 4, 10, 1, 22, tzinfo=<DstTzInfo 'US/Eastern' EDT-1 day, 20:00:00 DST>)


Maintainer
----------

* Lennart Regebro, regebro@gmail.com

Contributors
------------

* Marc Van Olmen
* Benjamen Meyer
* Manuel Ebert
* Xiaokun Zhu
* Cameris
* Edward Betts
* McK KIM
* Cris Ewing
* Ayala Shachar
* Lev Maximov
* Jakub Wilk
* John Quarles
* Preston Landers
* Victor Torres
* Jean Jordaan
* Zackary Welch
* Mickaël Schoentgen
* Gabriel Corona

(Sorry if I forgot someone)

License
-------

* MIT https://opensource.org/licenses/MIT
