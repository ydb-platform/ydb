  **Note**

  **Databricks recommends that you use newer Databricks CLI versions 0.200 and above instead of legacy Databricks CLI versions 0.17 and below (located in this repository). You can find the new CLI at https://github.com/databricks/cli.**

  **If you have been using the code in this repository as a Python SDK for interacting with the Databricks APIs, we recommend you use the new dedicated SDK package that can be found at https://github.com/databricks/databricks-sdk-py.**

databricks-cli
==============
.. image:: https://github.com/databricks/databricks-cli/actions/workflows/push.yml/badge.svg?branch=main
  :target: https://github.com/databricks/databricks-cli/actions/workflows/push.yml?query=branch%main
  :alt: Build status
.. image:: https://codecov.io/gh/databricks/databricks-cli/branch/main/graph/badge.svg
  :target: https://codecov.io/gh/databricks/databricks-cli


The Databricks Command Line Interface (CLI) is an open source tool which provides an easy to use interface to
the Databricks platform. The CLI is built on top of the Databricks REST APIs.

**Note**: This CLI is no longer under active development and has been released as an experimental client.

Please leave bug reports as issues on our `GitHub project <https://github.com/databricks/databricks-cli>`_.

Requirements
------------

-  Python Version >= 3.7

Installation
---------------

To install simply run
``pip install --upgrade databricks-cli``

Then set up authentication using username/password or `authentication token <https://docs.databricks.com/api/latest/authentication.html#token-management>`_. Credentials are stored at ``~/.databrickscfg``.

- ``databricks configure`` (enter hostname/username/password at prompt)
- ``databricks configure --token`` (enter hostname/auth-token at prompt)

Multiple connection profiles are also supported with ``databricks configure --profile <profile> [--token]``.
The connection profile can be used as such: ``databricks workspace ls --profile <profile>``.

To test that your authentication information is working, try a quick test like ``databricks workspace ls``.

Known Issues
---------------
``AttributeError: 'module' object has no attribute 'PROTOCOL_TLSv1_2'``

The Databricks web service requires clients speak TLSV1.2. The built in
version of Python for MacOS does not have this version of TLS built in.

To use the Databricks CLI you must install a version of Python that has ``ssl.PROTOCOL_TLSv1_2``.
For MacOS, the easiest way may be to install Python with `Homebrew <https://brew.sh/>`_.

Using Docker
------------
.. code::

    # build image
    docker build -t databricks-cli .

    # run container
    docker run -it databricks-cli

    # run command in docker
    docker run -it databricks-cli fs --help

Documentation
-------------

For the latest CLI documentation, see

- `Databricks <https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html>`_
- `Azure Databricks <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli>`_
