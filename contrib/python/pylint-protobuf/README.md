pylint-protobuf
===============

`pylint-protobuf` is a Pylint plugin for making Pylint aware of generated
fields from Protobuf types.

## Install

Install from PyPI via:

    pip install pylint-protobuf

Add or update `pylintrc` to load `pylint-protobuf`:

    [MASTER]
    load-plugins=pylint_protobuf

## Example

    $ cat <<EOF >person.proto
    message Person {
      required string name = 1;
    }
    EOF
    $ protoc person.proto --python_out=.
    $ cat <<EOF >readme.py
    from person_pb2 import Person
    p = Person('all arguments must be kwargs')
    p.invalid_field = 'value'
    p.name = 123
    EOF
    $ pip install pylint-protobuf
    $ pylint --load-plugins=pylint_protobuf readme.py
    ************* Module readme
    readme.py:2:4: E5904: Positional arguments are not allowed in message constructors and will raise TypeError (protobuf-no-posargs)
    readme.py:3:0: E5901: Field 'invalid_field' does not appear in the declared fields of protobuf-generated class 'Person' and will raise AttributeError on access (protobuf-undefined-attribute)
    readme.py:4:0: E5903: Field "Person.name" is of type 'str' and value 123 will raise TypeError at runtime (protobuf-type-error)

## Supported Python Versions

`pylint-protobuf` supports Python 3.8 at a minimum.

## Known Issues

`pylint-protobuf` does not currently support the following concepts:

* Some features of extensions: non-nested extensions, HasExtension(),
  ClearExtension(), type checking

## Alternatives

### mypy-protobuf

A `protoc` compiler plugin for generating `.pyi` stubs from `.proto` files.
Fully-featured and well supported, a useful extension to a `mypy` workflow.
May be better suited to your usecase if you control the entire pipeline. May
_not_ be suited if you are a downstream consumer of generated `_pb2.py` modules
with no access to the original `.proto` definitions, in which case
`pylint-protobuf` may be better suited for your use.
