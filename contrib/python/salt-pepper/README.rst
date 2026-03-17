======
Pepper
======

.. image:: https://img.shields.io/pypi/v/salt-pepper.svg
   :target: https://pypi.org/project/salt-pepper
.. image:: https://travis-ci.com/saltstack/pepper.svg?branch=develop
   :target: https://travis-ci.com/saltstack/pepper
.. image:: https://img.shields.io/pypi/pyversions/salt-pepper.svg
   :target: https://pypi.org/project/salt-pepper
.. image:: https://img.shields.io/badge/license-Apache2-blue.svg?maxAge=3600
   :target: https://pypi.org/project/salt-pepper
.. image:: https://codecov.io/gh/saltstack/pepper/branch/develop/graph/badge.svg
   :target: https://codecov.io/gh/saltstack/pepper/branch/develop

Pepper contains a Python library and CLI scripts for accessing a remote
`salt-api`__ instance.

``pepperlib`` abstracts the HTTP calls to ``salt-api`` so existing Python
projects can easily integrate with a remote Salt installation just by
instantiating a class.

The ``pepper`` CLI script allows users to execute Salt commands from computers
that are external to computers running the ``salt-master`` or ``salt-minion``
daemons as though they were running Salt locally. The long-term goal is to add
additional CLI scripts maintain the same interface as Salt's own CLI scripts
(``salt``, ``salt-run``, ``salt-key``, etc).

It does not require any additional dependencies and runs on Python 2.5+ and
Python 3. (Python 3 support is new, please file an issue if you encounter
trouble.)

.. __: https://github.com/saltstack/salt-api

Installation
------------
.. code-block:: bash

    pip install salt-pepper

Usage
-----

Basic usage is in heavy flux. You can run pepper using the script in %PYTHONHOME%/scripts/pepper (a pepper.cmd wrapper is provided for convenience to Windows users).

.. code-block:: bash

    export SALTAPI_USER=saltdev SALTAPI_PASS=saltdev SALTAPI_EAUTH=pam
    pepper '*' test.ping
    pepper '*' test.kwarg hello=dolly

Examples leveraging the runner client.

.. code-block:: bash

    pepper --client runner reactor.list
    pepper --client runner reactor.add event='test/provision/*' reactors='/srv/salt/state/reactor/test-provision.sls'

Configuration
-------------

You can configure pepper through the command line, using environment variables 
or in a configuration file ``$HOME/.pepperrc`` with the following syntax : 

.. code-block:: 

  [main]
  SALTAPI_URL=https://localhost:8000/
  SALTAPI_USER=saltdev
  SALTAPI_PASS=saltdev
  SALTAPI_EAUTH=pam

Contributing
------------

Please feel free to get involved by sending pull requests or join us on the
Salt mailing list or on IRC in #salt or #salt-devel.

This repo follows the same `contributing guidelines`__ as Salt and uses
separate develop and master branches for in-progress additions and bug-fix
changes respectively.

.. __: https://docs.saltstack.com/en/latest/topics/development/contributing.html
