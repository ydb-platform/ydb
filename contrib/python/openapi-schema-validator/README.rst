************************
openapi-schema-validator
************************

.. image:: https://img.shields.io/pypi/v/openapi-schema-validator.svg
     :target: https://pypi.python.org/pypi/openapi-schema-validator
.. image:: https://travis-ci.org/p1c2u/openapi-schema-validator.svg?branch=master
     :target: https://travis-ci.org/p1c2u/openapi-schema-validator
.. image:: https://img.shields.io/codecov/c/github/p1c2u/openapi-schema-validator/master.svg?style=flat
     :target: https://codecov.io/github/p1c2u/openapi-schema-validator?branch=master
.. image:: https://img.shields.io/pypi/pyversions/openapi-schema-validator.svg
     :target: https://pypi.python.org/pypi/openapi-schema-validator
.. image:: https://img.shields.io/pypi/format/openapi-schema-validator.svg
     :target: https://pypi.python.org/pypi/openapi-schema-validator
.. image:: https://img.shields.io/pypi/status/openapi-schema-validator.svg
     :target: https://pypi.python.org/pypi/openapi-schema-validator

About
#####

Openapi-schema-validator is a Python library that validates schema against the `OpenAPI Schema Specification v3.0 <https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.0.md#schemaObject>`__ which is an extended subset of the `JSON Schema Specification Wright Draft 00 <http://json-schema.org/>`__.

Installation
############

Recommended way (via pip):

::

    $ pip install openapi-schema-validator

Alternatively you can download the code and install from the repository:

.. code-block:: bash

   $ pip install -e git+https://github.com/p1c2u/openapi-schema-validator.git#egg=openapi_schema_validator


Usage
#####

Simple usage

.. code-block:: python

   from openapi_schema_validator import validate

   # A sample schema
   schema = {
       "type" : "object",
       "required": [
          "name"
       ],
       "properties": {
           "name": {
               "type": "string"
           },
           "age": {
               "type": "integer",
               "format": "int32",
               "minimum": 0,
               "nullable": True,
           },
           "birth-date": {
               "type": "string",
               "format": "date",
           }
       },
       "additionalProperties": False,
   }

   # If no exception is raised by validate(), the instance is valid.
   validate({"name": "John", "age": 23}, schema)

   validate({"name": "John", "city": "London"}, schema)

   Traceback (most recent call last):
       ...
   ValidationError: Additional properties are not allowed ('city' was unexpected)

You can also check format for primitive types

.. code-block:: python

   from openapi_schema_validator import oas30_format_checker

   validate({"name": "John", "birth-date": "-12"}, schema, format_checker=oas30_format_checker)

   Traceback (most recent call last):
       ...
   ValidationError: '-12' is not a 'date'


Related projects
################
* `openapi-core <https://github.com/p1c2u/openapi-core>`__
* `openapi-spec-validator <https://github.com/p1c2u/openapi-spec-validator>`__
