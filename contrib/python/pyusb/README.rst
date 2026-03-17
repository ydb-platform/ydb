==================================
PyUSB – Easy USB access for Python
==================================

Introduction
------------

PyUSB provides for easy access to the host machine's Universal Serial Bus (USB)
system for Python 3.

Until 0.4 version, PyUSB used to be a thin wrapper over libusb. Starting with
the 1.0 version, things changed considerably: now PyUSB is an API rich, backend
neutral Python USB module easy to use.

Documentation
-------------

The best way to get started with PyUSB is to read the following documents:

* `Tutorial`_
* `FAQ`_


For more detailed information, PyUSB's API documentation, as with most Python
modules, is based on Python doc strings and can be manipulated by tools such as
pydoc [1]_::

    $ python -m pydoc usb

The `libusb 1.0 documentation`_ is also a recommended read, especially when
using that backend (more on this below).

Requirements and platform support
---------------------------------

PyUSB is primarily developed and tested on Linux and Windows, but it should
also work fine on any platform running Python >= 3.9, ctypes and at least one
of the built-in backends.

PyUSB supports `libusb 1.0`_, libusb 0.1 and OpenUSB. Of those, libusb 1.0 is
currently recommended for most use cases.

*On Linux and BSD,* these will generally be available on the distribution's
official repositories.

*On macOS,* libusb 1.0 can easily be installed through Homebrew::

    $ brew install libusb

*On Windows,* `pyocd/libusb-package`_ is a convenient [2]_ [3]_ way to
provide the necessary libusb 1.0 DLL, as well as a suitable PyUSB backend and
a easy to use wrapper over PyUSB's ``find()`` API::

    # with pure PyUSB
    for dev in usb.core.find(find_all=True):
        print(dev)

    # with pyocd/libusb-package
    for dev in libusb_package.find(find_all=True):
        print(dev)


Alternatively, the libusb 1.0 DLL can be manually copied from an official
release archive into the ``C:\Windows\System32`` system folder, or packaged
together with the complete application.

Installing
----------

PyUSB is generally installed through pip [1]_::

    # the latest official release
    python -m pip install pyusb

    # a specific version (replace <version> with the desired version)
    python -m pip install pyusb==<version>

    # or the latest snapshop from the official git repository
    python -m pip install pyusb git+https://github.com/pyusb/pyusb#egg=pyusb

Most Linux distributions also package PyUSB in their official repositories.

Getting help
------------

If you have a question about PyUSB:

* consult the `FAQ`_;
* post a question in the `Q&A section`_;
* write to the `PyUSB mailing list`_.

To report a bug or propose a new feature, use our `issue tracker`_.  But please
search the database before opening a new issue.

Footnotes
---------

.. [1] On systems that still default to Python 2, replace ``python`` with
   ``python3``.

.. [2] Unlike PyUSB, pyocd/libusb-package uses the more restrictive Apache 2.0
   license.

.. [3] While pyocd/libusb-package supports platforms other than Windows,
   there are advantages to sticking to a system-provided libusb, if it is
   available and the platform has a robust package manager (e.g. Linux, BSD,
   macOS with Homebrew).

.. _FAQ: https://github.com/pyusb/pyusb/blob/master/docs/faq.rst
.. _PyUSB mailing list: https://sourceforge.net/projects/pyusb/lists/pyusb-users
.. _Q&A section: https://github.com/pyusb/pyusb/discussions/categories/q-a
.. _Tutorial: https://github.com/pyusb/pyusb/blob/master/docs/tutorial.rst
.. _issue tracker: https://github.com/pyusb/pyusb/issues
.. _libusb 1.0 documentation: https://libusb.info/
.. _libusb 1.0: https://github.com/libusb/libusb
.. _pyocd/libusb-package: https://github.com/pyocd/libusb-package/
