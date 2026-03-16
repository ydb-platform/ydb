URLPY
=====

urlpy is a small library for URL parsing, cleanup, canonicalization and equivalence.
You can find it at https://github.com/nexB/urlpy
urlpy is MIT-licensed.

urlpy is derived from Moz's url.py v0.2.0 and has been simplified to run on
Python 2 and Python 3 using a pure Python library. (Newer version of Moz's
url.py use a C++ extension).

At the heart of the `urlpy` package is the `URL` object. You can get one by
passing in a unicode or string object into the top-level `parse` method. All
strings asre assumed to be Unicode::

    import urlpy
    myurl = url.parse('http://foo.com')

The workflow is that you'll chain a number of permutations together to get the type
of URL you're after::

    # Defrag, remove some parameters and give me a string
    str(urlpy.parse(...).defrag().deparam(['utm_source']))

    # Escape the path, and punycode the host, and give me a string
    str(urlpy.parse(...).escape().punycode())

    # Give me the absolute path url as some encoding
    str(urlpy.parse(...).abspath()).encode('some encoding')


URL Equivalence
===============

URL objects compared with `==` are interpreted very strictly, but for a more
lax interpretation, consider using `equiv` to test if two urls are functionally
equivalent::

    a = urlpy.parse(u'https://föo.com:443/a/../b/.?b=2&&&&&&a=1')
    b = urlpy.parse(u'https://xn--fo-fka.COM/b/?a=1&b=2')

    # These urls are not equal
    assert(a != b)

    # But they are equivalent
    assert(a.equiv(b))
    assert(b.equiv(a))

This equivalence test takes default ports for common schemes into account (so
if both urls are the same scheme, but one explicitly specifies the default
port), punycoding, case of the host name, and parameter order.


Absolute URLs
=============

You can perform many operations on relative urls (those without a hostname),
but punycoding and unpunycoding are not among them. You can also tell whether
or not a url is absolute::

    a = urlpy.parse('foo/bar.html')
    assert(not a.absolute())


Chaining
========

Many of the methods on the `URL` class can be chained to produce a number of
effects in sequence::

    import url

    # Create a url object
    myurl = urlpy.URL.parse('http://www.FOO.com/bar?utm_source=foo#what')
    # Remove some parameters and the fragment
    print(myurl.defrag().deparam(['utm_source']))

In fact, unless the function explicitly returns a string, then the method may
be chained.


`canonical`
-----------

According to the RFC, the order of parameters is not supposed to matter. In
practice, it can (depending on how the server matches URL routes), but it's
also helpful to be able to put parameters in a canonical ordering. This
ordering happens to be alphabetical order::

    >>> str(urlpy.parse('http://foo.com/?b=2&a=1&d=3').canonical())
    'http://foo.com/?a=1&b=2&d=3'


`defrag`
--------

Remove any fragment identifier from the url. This isn't part of the reuqest
that gets sent to an HTTP server, and so it's often useful to remove the 
fragment when doing url comparisons::

    >>> str(urlpy.parse('http://foo.com/#foo').defrag())
    'http://foo.com/'


`deparam`
---------

Some parameters are commonly added to urls that we may not be interested in. Or
they may be misleading. Common examples include referrering pages, `utm_source`
and session ids. To strip out all such parameters from your url::

    >>> str(urlpy.parse('http://foo.com/?do=1&not=2&want=3&this=4').deparam(['do', 'not', 'want']))
    'http://foo.com/?this=4'


`abspath`
---------

Like its `os.path` namesake, this makes sure that the path of the url is
absolute. This includes removing redundant forward slashes, `.` and `..`::

    >>> str(urlpy.parse('http://foo.com/foo/./bar/../a/b/c/../../d').abspath())
    'http://foo.com/foo/a/d'


`escape`
--------

Non-ASCII characters in the path are typically encoded as UTF-8 and then
escaped as `%HH` where `H` are hexidecimal values. It's important to note that
the `escape` function is idempotent, and can be called repeatedly::

    >>> str(urlpy.parse(u'http://foo.com/ümlaut').escape())
    'http://foo.com/%C3%BCmlaut'
    >>> str(urlpy.parse(u'http://foo.com/ümlaut').escape().escape())
    'http://foo.com/%C3%BCmlaut'


`unescape`
----------

If you have a URL that might have been escaped before it was given to you, but
you'd like to display something a little more meaningful than `%C3%BCmlaut`, 
you can unescape the path::

    >>> print(urlpy.parse('http://foo.com/%C3%BCmlaut').unescape())
    http://foo.com/ümlaut


Properties
==========

Many attributes are available on URL objects:

- `scheme` -- empty string if URL is relative
- `host` -- `None` if URL is relative
- `hostname` -- like `host`, but empty string if URL is relative
- `port` -- `None` if absent (or removed)
- `path` -- always with a leading `/`
- `params` -- string of params following the `;` (with extra `;`'s removed)
- `query` -- string of queries following the `?` (with extra `?`'s and `&`'s removed)
- `fragment` -- empty string if absent
- `absolute` -- a `bool` indicating whether the URL is absolute
- `unicode` -- a unicode version of the URL


Running tests
=============
::
    ./configure
    pytest


Authors
=======

- David Barts, Moz
- Brandon Forehand, Moz
- Dan Lecocq, Moz
- Philippe Ombredanne for nexB Inc.