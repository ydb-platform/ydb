Essentials
----------
IMAPClient is an easy-to-use, Pythonic and complete IMAP client
library.

=========================  ========================================
Current version            3.0.1
Supported Python versions  3.8 - 3.14
License                    New BSD
Project home               https://github.com/mjs/imapclient/
PyPI                       https://pypi.python.org/pypi/IMAPClient
Documentation              https://imapclient.readthedocs.io/
Discussions                https://github.com/mjs/imapclient/discussions
Test Status                |build master|
=========================  ========================================

.. |build master| image:: https://github.com/mjs/imapclient/actions/workflows/main.yml/badge.svg
   :target: https://github.com/mjs/imapclient/actions
   :alt: master branch

Features
--------
- Arguments and return values are natural Python types.
- IMAP server responses are fully parsed and readily usable.
- IMAP unique message IDs (UIDs) are handled transparently. There is
  no need to call different methods to use UIDs.
- Escaping for internationalised mailbox names is transparently
  handled.  Unicode mailbox names may be passed as input wherever a
  folder name is accepted.
- Time zones are transparently handled including when the server and
  client are in different zones.
- Convenience methods are provided for commonly used functionality.
- Exceptions are raised when errors occur.

Example
-------

.. code-block:: python

    from imapclient import IMAPClient

    # context manager ensures the session is cleaned up
    with IMAPClient(host="imap.host.org") as client:
        client.login('someone', 'secret')
        client.select_folder('INBOX')

        # search criteria are passed in a straightforward way
        # (nesting is supported)
        messages = client.search(['NOT', 'DELETED'])

        # fetch selectors are passed as a simple list of strings.
        response = client.fetch(messages, ['FLAGS', 'RFC822.SIZE'])

        # `response` is keyed by message id and contains parsed,
        # converted response items.
        for message_id, data in response.items():
            print('{id}: {size} bytes, flags={flags}'.format(
                id=message_id,
                size=data[b'RFC822.SIZE'],
                flags=data[b'FLAGS']))

Why IMAPClient?
---------------
You may ask: "why create another IMAP client library for Python?
Doesn't the Python standard library already have imaplib?".

The problem with imaplib is that it's very low-level. It expects
string values where lists or tuples would be more appropriate and
returns server responses almost unparsed. As IMAP server responses can
be quite complex this means everyone using imaplib ends up writing
their own fragile parsing routines.

Also, imaplib doesn't make good use of exceptions. This means you need
to check the return value of each call to imaplib to see if what you
just did was successful.

IMAPClient actually uses imaplib internally. This may change at some
point in the future.

Installing IMAPClient
---------------------
IMAPClient is listed on PyPI and can be installed with pip::

    pip install imapclient

More installation methods are described in the documentation.

Documentation
-------------
IMAPClient's manual is available at http://imapclient.readthedocs.io/.
Release notes can be found at
http://imapclient.readthedocs.io/#release-history.

See the `examples` directory in the root of project source for
examples of how to use IMAPClient.

Current Status
--------------
You should feel confident using IMAPClient for production purposes. 

In order to clearly communicate version compatibility, IMAPClient
will strictly adhere to the `Semantic Versioning <http://semver.org>`_
scheme from version 1.0 onwards.

The project's home page is https://github.com/mjs/imapclient/ (this
currently redirects to the IMAPClient Github site). Details about
upcoming versions and planned features/fixes can be found in the issue
tracker on Github. The maintainers also blog about IMAPClient
news. Those articles can be found `here
<http://menno.io/tags/imapclient>`_.

Discussions
-----------
`Github Discussions`_ can be used to ask questions, propose changes or praise
the project maintainers :)

.. _`Github Discussions`: https://github.com/mjs/imapclient/discussions

Working on IMAPClient
---------------------
The `contributing documentation
<http://imapclient.rtfd.io/en/master/contributing.html>`_ contains
information for those interested in improving IMAPClient.

IMAP Servers
------------
IMAPClient is heavily tested against Dovecot, Gmail, Fastmail.fm
(who use a modified Cyrus implementation), Office365 and Yahoo. Access
to accounts on other IMAP servers/services for testing would be
greatly appreciated.

Interactive Console
-------------------
This script connects an IMAPClient instance using the command line
args given and starts an interactive session. This is useful for
exploring the IMAPClient API and testing things out, avoiding the
steps required to set up an IMAPClient instance.

The IPython shell is used if it is installed. Otherwise the
code.interact() function from the standard library is used.

The interactive console functionality can be accessed running the
interact.py script in the root of the source tree or by invoking the
interact module like this::

    python -m imapclient.interact ...

"Live" Tests
------------
IMAPClient includes a series of live, functional tests which exercise
it against a live IMAP account. These are useful for ensuring
compatibility with a given IMAP server implementation.

The livetest functionality are run from the root of the project source
like this::

    python livetest.py <livetest.ini> [ optional unittest arguments ]

The configuration file format is
`described in the main documentation <http://imapclient.rtfd.io/#configuration-file-format>`_.

**WARNING**: The operations used by livetest are destructive and could
cause unintended loss of data. That said, as of version 0.9, livetest
limits its activity to a folder it creates and subfolders of that
folder. It *should* be safe to use with any IMAP account but please
don't run livetest against a truly important IMAP account.

Please include the output of livetest.py with an issue if it fails
to run successfully against a particular IMAP server. Reports of
successful runs are also welcome.  Please include the type and version
of the IMAP server, if known.
