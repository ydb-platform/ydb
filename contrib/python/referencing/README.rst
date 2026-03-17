===============
``referencing``
===============

|PyPI| |Pythons| |CI| |pre-commit|

.. |PyPI| image:: https://img.shields.io/pypi/v/referencing.svg
  :alt: PyPI version
  :target: https://pypi.org/project/referencing/

.. |Pythons| image:: https://img.shields.io/pypi/pyversions/referencing.svg
  :alt: Supported Python versions
  :target: https://pypi.org/project/referencing/

.. |CI| image:: https://github.com/python-jsonschema/referencing/workflows/CI/badge.svg
  :alt: Build status
  :target: https://github.com/python-jsonschema/referencing/actions?query=workflow%3ACI

.. |ReadTheDocs| image:: https://readthedocs.org/projects/referencing/badge/?version=stable&style=flat
   :alt: ReadTheDocs status
   :target: https://referencing.readthedocs.io/en/stable/

.. |pre-commit| image:: https://results.pre-commit.ci/badge/github/python-jsonschema/referencing/main.svg
  :alt: pre-commit.ci status
  :target: https://results.pre-commit.ci/latest/github/python-jsonschema/referencing/main


An implementation-agnostic implementation of JSON reference resolution.

In other words, a way for e.g. JSON Schema tooling to resolve the ``$ref`` keyword across all drafts (without needing to implement support themselves).

What's here is inspired in part by the budding JSON `reference specification(s) <https://github.com/json-schema-org/referencing>`_ (currently housed within the JSON Schema organization but intended to be more broadly applicable), which intend to detach some of the referencing behavior from JSON Schema's own specifications.

See `the documentation <https://referencing.readthedocs.io/>`_ for more details.
