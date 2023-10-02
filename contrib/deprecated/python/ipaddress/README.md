ipaddress
=========

Python 3.3+'s [ipaddress](http://docs.python.org/dev/library/ipaddress) for Python 2.6, 2.7, 3.2.

This repository tracks the latest version from cpython, e.g. ipaddress from cpython 3.8 as of writing.

Note that just like in Python 3.3+ you must use character strings and not byte strings for textual IP address representations:

```python
>>> from __future__ import unicode_literals
>>> ipaddress.ip_address('1.2.3.4')
IPv4Address(u'1.2.3.4')
```
or
```python
>>> ipaddress.ip_address(u'1.2.3.4')
IPv4Address(u'1.2.3.4')
```
but not:
```python
>>> ipaddress.ip_address(b'1.2.3.4')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "ipaddress.py", line 163, in ip_address
    ' a unicode object?' % address)
ipaddress.AddressValueError: '1.2.3.4' does not appear to be an IPv4 or IPv6 address. Did you pass in a bytes (str in Python 2) instead of a unicode object?
```
