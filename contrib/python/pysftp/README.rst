pysftp
======

A simple interface to SFTP.  The module offers high level abstractions and
task based routines to handle your SFTP needs.  Checkout the Cook Book, in the
docs, to see what pysftp can do for you.

Example
-------

::

    import pysftp

    with pysftp.Connection('hostname', username='me', password='secret') as sftp:
        with sftp.cd('public'):             # temporarily chdir to public
            sftp.put('/my/local/filename')  # upload file to public/ on remote
            sftp.get('remote_file')         # get a remote file


Supports
--------
Tested on Python 2.7, 3.2, 3.3, 3.4

.. image:: https://drone.io/bitbucket.org/dundeemt/pysftp/status.png
    :target: https://drone.io/bitbucket.org/dundeemt/pysftp/latest
    :alt: Build Status


* Project:  https://bitbucket.org/dundeemt/pysftp
* Download: https://pypi.python.org/pypi/pysftp
* Documentation: http://pysftp.rtfd.org/

