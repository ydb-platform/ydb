"""
dydantic
========

Dydantic is a Python library for dynamically generating Pydantic models from JSON schemas.
It provides a convenient way to create Pydantic models on-the-fly based on the structure
defined in a JSON schema. These can be used to assist in validating and parsing data
according to the schema rules.

Features
--------
- Automatically generate Pydantic models from JSON schemas
- Support for nested objects and referenced definitions
- Customizable model configurations, base classes, and validators
- Handle various JSON schema types and formats
- Extensible and flexible API

Installation
------------
You can install dydantic using pip:

    pip install -U dydantic

Usage
-----
Here's a simple example of how to use dydantic to create a Pydantic model from a JSON schema:

    from dydantic import create_model_from_schema

    json_schema = {
        "title": "Person",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }

    Person = create_model_from_schema(json_schema)

    person = Person(name="John", age=30)
    print(person)  # Output: Person(name='John', age=30)

For more advanced usage and examples, please refer to the documentation.

Documentation
-------------
The complete documentation for dydantic can be found at:
https://dydantic.readthedocs.io/

The documentation provides detailed information on installation, usage, API reference,
and examples.

Contributing
------------
Contributions to dydantic are welcome! If you find any issues or have suggestions for
improvements, please open an issue or submit a pull request on the GitHub repository:
https://github.com/hinthornw/dydantic

License
-------
dydantic is open-source software licensed under the MIT License.
See the LICENSE file for more information.

"""

from dydantic._utils import create_model_from_schema


__all__ = ["create_model_from_schema"]
