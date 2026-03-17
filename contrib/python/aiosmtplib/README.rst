aiosmtplib
==========

|circleci| |precommit.ci| |codecov| |zero-deps| |pypi-version| |downloads| |pypi-license|

------------

aiosmtplib is an asynchronous SMTP client for use with asyncio.

For documentation, see `Read The Docs`_.

Quickstart
----------


..
  start quickstart

.. code-block:: python

    import asyncio
    from email.message import EmailMessage

    import aiosmtplib

    message = EmailMessage()
    message["From"] = "root@localhost"
    message["To"] = "somebody@example.com"
    message["Subject"] = "Hello World!"
    message.set_content("Sent via aiosmtplib")

    asyncio.run(aiosmtplib.send(message, hostname="127.0.0.1", port=25))

..
  end quickstart

Requirements
------------

..
  start requirements

Python 3.10+ is required.

..
  end requirements


Bug Reporting
-------------

..
  start bug-reporting

Bug reports (and feature requests) are welcome via `Github issues`_.

.. _Github issues: https://github.com/cole/aiosmtplib/issues

..
  end bug-reporting


.. |circleci| image:: https://circleci.com/gh/cole/aiosmtplib/tree/main.svg?style=shield
           :target: https://circleci.com/gh/cole/aiosmtplib/tree/main
           :alt: "aiosmtplib CircleCI build status"
.. |pypi-version| image:: https://img.shields.io/pypi/v/aiosmtplib.svg
                 :target: https://pypi.python.org/pypi/aiosmtplib
                 :alt: "aiosmtplib on the Python Package Index"
.. |pypi-status| image:: https://img.shields.io/pypi/status/aiosmtplib.svg
.. |pypi-license| image:: https://img.shields.io/pypi/l/aiosmtplib.svg
.. |codecov| image:: https://codecov.io/gh/cole/aiosmtplib/branch/main/graph/badge.svg
             :target: https://codecov.io/gh/cole/aiosmtplib
.. |downloads| image:: https://static.pepy.tech/badge/aiosmtplib/month
               :target: https://pepy.tech/project/aiosmtplib
               :alt: "aiosmtplib on pypy.tech"
.. |precommit.ci| image:: https://results.pre-commit.ci/badge/github/cole/aiosmtplib/main.svg
                  :target: https://results.pre-commit.ci/latest/github/cole/aiosmtplib/main
                  :alt: "pre-commit.ci status"
.. |zero-deps| image:: https://0dependencies.dev/0dependencies.svg
               :target: https://0dependencies.dev
               :alt: "0 dependencies"
.. _Read The Docs: https://aiosmtplib.readthedocs.io/en/stable/
