.. image:: https://img.shields.io/pypi/v/schwifty.svg?style=flat-square
    :target: https://pypi.python.org/pypi/schwifty
.. image:: https://img.shields.io/github/actions/workflow/status/mdomke/schwifty/lint-and-test.yml?branch=main&style=flat-square
    :target: https://github.com/mdomke/schwifty/actions?query=workflow%3Alint-and-test
.. image:: https://img.shields.io/pypi/l/schwifty.svg?style=flat-square
    :target: https://pypi.python.org/pypi/schwifty
.. image:: https://readthedocs.org/projects/schwifty/badge/?version=latest&style=flat-square
    :target: https://schwifty.readthedocs.io
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square
    :target: https://black.readthedocs.io/en/stable/index.html
.. image:: https://img.shields.io/codecov/c/gh/mdomke/schwifty?token=aJj1Yg0NUq&style=flat-square
    :target: https://codecov.io/gh/mdomke/schwifty


Gotta get schwifty with your IBANs
==================================

.. teaser-begin

``schwifty`` is a Python library that let's you easily work with IBANs and BICs
as specified by the ISO. IBAN is the Internation Bank Account Number and BIC
the Business Identifier Code. Both are used for international money transfer.

Features
--------

``schwifty`` lets you

* `validate`_ check-digits and the country specific format of IBANs
* `validate`_ format and country codes from BICs
* `generate`_ BICs from country and bank-code
* `generate`_ IBANs from country-code, bank-code and account-number.
* `generate`_ random valid IBANs
* get the BIC associated to an IBAN's bank-code
* access all relevant components as attributes

See the `docs <https://schwifty.readthedocs.io>`_ for more inforamtion.

.. _validate: https://schwifty.readthedocs.io/en/latest/examples.html#validation
.. _generate: https://schwifty.readthedocs.io/en/latest/examples.html#generation

.. teaser-end

Versioning
----------

Since the IBAN specification and the mapping from BIC to bank_code is updated from time to time,
``schwifty`` uses `CalVer <http://www.calver.org/>`_ for versioning with the scheme ``YY.0M.Micro``.


.. installation-begin

Installation
------------

To install ``schwifty``, simply:

.. code-block:: bash

  $ pip install schwifty

.. installation-end


Development
-----------

We use the `black`_ as code formatter. This avoids discussions about style preferences in the same
way as ``gofmt`` does the job for Golang. The conformance to the formatting rules is checked in the
CI pipeline, so that it is recommendable to install the configured `pre-commit`_-hook, in order to
avoid long feedback-cycles.

.. code-block:: bash

   $ pre-commit install

You can also use the ``fmt`` Makefile-target to format the code or use one of the available `editor
integrations`_.


Project Information
-------------------

``schwifty`` is released under `MIT`_ license and its documentation lives at `Read the Docs`_. The
code is maintained on `GitHub`_ and packages are distributed on `PyPI`_

Name
~~~~

Since ``swift`` and ``swiftly`` were already taken by the OpenStack-project, but we somehow wanted
to point out the connection to SWIFT, Rick and Morty came up with the idea to name the project
``schwifty``.

.. image:: https://i.cdn.turner.com/adultswim/big/video/get-schwifty-pt-2/rickandmorty_ep205_002_vbnuta15a755dvash8.jpg


.. _black: https://black.readthedocs.io/en/stable/index.html
.. _pre-commit: https://pre-commit.com
.. _editor integrations:  https://black.readthedocs.io/en/stable/editor_integration.html
.. _MIT: https://choosealicense.com/licenses/mit/
.. _Read the Docs: https://schwifty.readthedocs.io
.. _GitHub: https://github.com/mdomke/schwifty
.. _PyPI: https://pypi.org/project/schwifty
