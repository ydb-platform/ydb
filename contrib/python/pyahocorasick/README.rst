========================================================================
                          pyahocorasick
========================================================================


|build-ghactions| |docs|


**pyahocorasick** is a fast and memory efficient library for exact or approximate
multi-pattern string search meaning that you can find multiple key strings
occurrences at once in some input text.  The strings "index" can be built ahead
of time and saved (as a pickle) to disk to reload and reuse later.  The library
provides an `ahocorasick` Python module that you can use as a plain dict-like
Trie or convert a Trie to an automaton for efficient Aho-Corasick search.

**pyahocorasick** is implemented in C and tested on Python 3.9 and up.
It works on 64 bits Linux, macOS and Windows.

The license_ is BSD-3-Clause. Some utilities, such as tests and the pure Python
automaton are dedicated to the Public Domain.


Testimonials
=============

`Many thanks for this package. Wasn't sure where to leave a thank you note but
this package is absolutely fantastic in our application where we have a library
of 100k+ CRISPR guides that we have to count in a stream of millions of DNA
sequencing reads. This package does it faster than the previous C program we
used for the purpose and helps us stick to just Python code in our pipeline.`

Miika (AstraZeneca Functional Genomics Centre)
https://github.com/WojciechMula/pyahocorasick/issues/145


Download and source code
========================

You can fetch **pyahocorasick** from:

- GitHub https://github.com/WojciechMula/pyahocorasick/
- Pypi https://pypi.python.org/pypi/pyahocorasick/
- Conda-Forge https://github.com/conda-forge/pyahocorasick-feedstock/

The **documentation** is published at https://pyahocorasick.readthedocs.io/


Quick start
===========

This module is written in C. You need a C compiler installed to compile native
CPython extensions. To install::

    pip install pyahocorasick

Then create an Automaton::

    >>> import ahocorasick
    >>> automaton = ahocorasick.Automaton()

You can use the Automaton class as a trie. Add some string keys and their associated
value to this trie. Here we associate a tuple of (insertion index, original string)
as a value to each key string we add to the trie::

    >>> for idx, key in enumerate('he her hers she'.split()):
    ...   automaton.add_word(key, (idx, key))

Then check if some string exists in the trie::

    >>> 'he' in automaton
    True
    >>> 'HER' in automaton
    False

And play with the ``get()`` dict-like method::

    >>> automaton.get('he')
    (0, 'he')
    >>> automaton.get('she')
    (3, 'she')
    >>> automaton.get('cat', 'not exists')
    'not exists'
    >>> automaton.get('dog')
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    KeyError

Now convert the trie to an Aho-Corasick automaton to enable Aho-Corasick search::

    >>> automaton.make_automaton()

Then search all occurrences of the keys (the needles) in an input string (our haystack).

Here we print the results and just check that they are correct. The
`Automaton.iter()` method return the results as two-tuples of the `end index` where a
trie key was found in the input string and the associated `value` for this key. Here
we had stored as values a tuple with the original string and its trie insertion
order::

    >>> for end_index, (insert_order, original_value) in automaton.iter(haystack):
    ...     start_index = end_index - len(original_value) + 1
    ...     print((start_index, end_index, (insert_order, original_value)))
    ...     assert haystack[start_index:start_index + len(original_value)] == original_value
    ...
    (1, 2, (0, 'he'))
    (1, 3, (1, 'her'))
    (1, 4, (2, 'hers'))
    (4, 6, (3, 'she'))
    (5, 6, (0, 'he'))

You can also create an eventually large automaton ahead of time and `pickle` it to
re-load later. Here we just pickle to a string. You would typically pickle to a
file instead::

    >>> import pickle
    >>> pickled = pickle.dumps(automaton)
    >>> B = pickle.loads(pickled)
    >>> B.get('he')
    (0, 'he')


See also:

* FAQ and Who is using pyahocorasick? 
  https://github.com/WojciechMula/pyahocorasick/wiki/FAQ#who-is-using-pyahocorasick


Documentation
=============

The full documentation including the API overview and reference is published on
`readthedocs <http://pyahocorasick.readthedocs.io/>`_.


Overview

With an `Aho-Corasick automaton <http://en.wikipedia.org/wiki/Aho-Corasick%20algorithm>`_
you can efficiently search all occurrences of multiple strings (the needles) in an
input string (the haystack) making a single pass over the input string. With
pyahocorasick you can eventually build large automatons and pickle them to reuse
them over and over as an indexed structure for fast multi pattern string matching.

One of the advantages of an Aho-Corasick automaton is that the typical worst-case
and best-case **runtimes** are about the same and depends primarily on the size
of the input string and secondarily on the number of matches returned.  While
this may not be the fastest string search algorithm in all cases, it can search
for multiple strings at once and its runtime guarantees make it rather unique.
Because pyahocorasick is based on a Trie, it stores redundant keys prefixes only
once using memory efficiently.

A drawback is that it needs to be constructed and "finalized" ahead of time
before you can search strings. In several applications where you search for
several pre-defined "needles" in a variable "haystacks" this is actually an
advantage.

**Aho-Corasick automatons** are commonly used for fast multi-pattern matching
in intrusion detection systems (such as snort), anti-viruses and many other
applications that need fast matching against a pre-defined set of string keys.

Internally an Aho-Corasick automaton is typically based on a Trie with extra
data for failure links and an implementation of the Aho-Corasick search
procedure.

Behind the scenes the **pyahocorasick** Python library implements these two data
structures:  a `Trie <http://en.wikipedia.org/wiki/trie>`_ and an Aho-Corasick
string matching automaton. Both are exposed through the `Automaton` class.

In addition to Trie-like and Aho-Corasick methods and data structures,
**pyahocorasick** also implements dict-like methods: The pyahocorasick
**Automaton** is a **Trie** a dict-like structure indexed by string keys each
associated with a value object. You can use this to retrieve an associated value
in a time proportional to a string key length.

pyahocorasick is available in two flavors:

* a CPython **C-based extension**, compatible with Python 3 only. Use older
  version 1.4.x for Python 2.7.x and 32 bits support.

* a simpler pure Python module, compatible with Python 2 and 3. This is only
  available in the source repository (not on Pypi) under the etc/py/ directory
  and has a slightly different API.


Unicode and bytes
-----------------

The type of strings accepted and returned by ``Automaton`` methods are either
**unicode** or **bytes**, depending on a compile time settings (preprocessor
definition of ``AHOCORASICK_UNICODE`` as set in `setup.py`).

The ``Automaton.unicode`` attributes can tell you how the library was built.
On Python 3, unicode is the default.


.. warning::

    When the library is built with unicode support, an Automaton will store 2 or
    4 bytes per letter, depending on your Python installation. When built
    for bytes, only one byte per letter is needed.


Build and install from PyPi
===========================

To install for common operating systems, use pip. Pre-built wheels should be
available on Pypi at some point in the future::

    pip install pyahocorasick

To build from sources you need to have a C compiler installed and configured which
should be standard on Linux and easy to get on MacOSX.

To build from sources, clone the git repository or download and extract the source
archive.

Install `pip` (and its `setuptools` companion) and then run (in a `virtualenv` of
course!)::

    pip install .

If compilation succeeds, the module is ready to use.


Support
=======

Support is available through the `GitHub issue tracker
<https://github.com/WojciechMula/pyahocorasick/issues>`_ to report bugs or ask
questions.


Contributing
============

You can submit contributions through `GitHub pull requests
<https://github.com/WojciechMula/pyahocorasick/pull>`_.

- There is a Makefile with a default target that builds and runs tests.
- The tests can run with a `pip installe -e .[testing] && pytest -vvs`
- See also the .github directory for CI tests and workflow


Authors
=======

The initial author and maintainer is Wojciech Muła. `Philippe Ombredanne
<https://github.com/pombredanne>`_ is Wojciech's sidekick and helps maintaining,
and rewrote documentation, setup CI servers and did a some work to make this
module more accessible to end users.

Alphabetic list of authors and contributors:

* **Andrew Grigorev**
* **Ayan Mahapatra**
* **Bogdan**
* **David Woakes**
* **Edward Betts**
* **Frankie Robertson**
* **Frederik Petersen**
* **gladtosee**
* **INADA Naoki**
* **Jan Fan**
* **Pastafarianist**
* **Philippe Ombredanne**
* **Renat Nasyrov**
* **Sylvain Zimmer**
* **Xiaopeng Xu**

and many others!

This library would not be possible without help of many people, who contributed in
various ways.
They created `pull requests <https://github.com/WojciechMula/pyahocorasick/pull>`_,
reported bugs as `GitHub issues <https://github.com/WojciechMula/pyahocorasick/issues>`_
or via direct messages, proposed fixes, or spent their valuable time on testing.

Thank you.


License
=======

This library is licensed under very liberal
`BSD-3-Clause <http://spdx.org/licenses/BSD-3-Clause.html>`_ license. Some
portions of the code are dedicated to the public domain such as the pure Python
automaton and test code.

Full text of license is available in LICENSE file.


Other Aho-Corasick implementations for Python you can consider
==============================================================

While **pyahocorasick** tries to be the finest and fastest Aho Corasick library
for Python you may consider these other libraries:


* `py_aho_corasick <https://github.com/JanFan/py-aho-corasick>`_ by Jan

 * Written in pure Python.
 * Poor performance.

* `ahocorapy <https://github.com/abusix/ahocorapy>`_ by abusix

 * Written in pure Python.
 * Better performance than py-aho-corasick.
 * Using pypy, ahocorapy's search performance is only slightly worse than pyahocorasick's.
 * Performs additional suffix shortcutting (more setup overhead, less search overhead for suffix lookups).
 * Includes visualization tool for resulting automaton (using pygraphviz).
 * MIT-licensed, 100% test coverage, tested on all major python versions (+ pypy)

* `noaho <https://github.com/JDonner/NoAho>`_ by Jeff Donner

 * Written in C. Does not return overlapping matches.
 * Does not compile on Windows (July 2016).
 * No support for the pickle protocol.

* `acora <https://github.com/scoder/acora>`_ by Stefan Behnel

 * Written in Cython.
 * Large automaton may take a long time to build (July 2016)
 * No support for a dict-like protocol to associate a value to a string key.

* `ahocorasick <https://hkn.eecs.berkeley.edu/~dyoo/python/ahocorasick/>`_ by Danny Yoo

 * Written in C.
 * seems unmaintained (last update in 2005).
 * GPL-licensed.


.. |build-ghactions| image:: https://github.com/WojciechMula/pyahocorasick/actions/workflows/test-and-build.yml/badge.svg
   :target: https://github.com/WojciechMula/pyahocorasick/actions/workflows/test-and-build.yml
   :alt: GitHub Action build on test -  Master branch status

.. |docs| image:: https://readthedocs.org/projects/pyahocorasick/badge/?version=latest
   :alt: Documentation Status
   :target: https://pyahocorasick.readthedocs.io/en/latest/
