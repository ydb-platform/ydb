# Plugin for mypy to support zope.interface

[![Coverage Status](https://coveralls.io/repos/github/Shoobx/mypy-zope/badge.svg)](https://coveralls.io/github/Shoobx/mypy-zope)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

The goal is to be able to make zope interfaces to be treated as types in mypy
sense.

## Usage

Install both mypy and mypy-zope:
```sh
pip install mypy-zope
```

Edit `mypy.ini` file in your project to enable the plugin:

```ini
[mypy]
namespace_packages=True
plugins=mypy_zope:plugin
```

You're done! You can now check your project with mypy:

```sh
mypy your-project-dir
```

## What is supported?

You can browse
[sample files](https://github.com/Shoobx/mypy-zope/tree/master/tests/samples)
to get some sense on what features are supported and how they are handled.

### Interface declarations

You can define the interface and provide implementation:

```python
class IAnimal(zope.interface.Interface):
    def say() -> None:
        pass

@zope.interface.implementer(IAnimal)
class Cow(object):
    def say(self) -> None:
        print("Moooo")

animal: IAnimal = Cow()
animal.say()
```

The interface `IAnimal` will be treated as superclass of the implementation
`Cow`: you will be able to pass an implementation to functions accepting an
interface and all the usual polymorphism tricks.

It is also possible to declare the implementation using `classImplements`
function  with the same effect as `@imlementer` decorator. This is useful if
you do not control the code that defines the implementation class.

```python
classImplements(Cow, IAnimal)

animal: IAnimal = Cow()
```

### Schema field type inference
A limited support for defining attributes as `zope.schema.Field`s is supported too:

```python
class IAnimal(zope.interface.Interface):
    number_of_legs = zope.schema.Int(title="Number of legs")

@zope.interface.implementer(IAnimal)
class Cow(object):
    number_of_legs = 4
```

In context of an interface, some known `zope.schema` field types are
automatically translated to python types, so the `number_of_legs` attributes is
getting the type `int` in the example above. That means mypy will report an
error if you try to assign string to that attribute on an instance of `IAnimal`
type. Custom fields or fields not recognized by plugin are given type `Any`.

### Field properties

Support for `zope.schema.FieldProperty` is limited, because type information is
not transferred from an interface to implementation attribute, but mypy doesn't
report errors on sources like this:

```python
class IAnimal(zope.interface.Interface):
    number_of_legs = zope.schema.Int(title="Number of legs")

@zope.interface.implementer(IAnimal)
class Cow(object):
    number_of_legs = zope.schema.FieldProperty(IAnimal['number_of_legs'])
```

The type of `Cow.number_of_legs` will become `Any` in this case, even though
`IAnimal.number_of_legs` would be inferred as `int`.

### Adaptation pattern

Zope interfaces can be "called" to lookup an adapter, like this:

```python
class IEUPowerSocket(zope.interface.Interface):
    def fit():
        pass

adapter = IEUPowerSocket(us_plug)
adapter.fit()
```

Type of the `adapter` variable will be set to `IEUPowerSocket`.

### Conditional type inference

When using `zope.interface`'s `implementedBy()` and `providedBy()` methods
in an if statement, `mypy` will know which type it is inside those statements.

```python
if IAnimal.providedBy(ob):
    ob.number_of_legs += 2

```

### Declaration of overloaded methods in interfaces

Similarly to regular [overloaded
functions](https://docs.python.org/3/library/typing.html#typing.overload),
`@overload` declarations are supported in interfaces as well:

```python
class IAnimal(zope.interface.Interface):
    @overload
    def say() -> str:
        ...

    @overload
    def say(count: int) -> List[str]:
        ...

    def say(count: int = None) -> Union[str, List[str]]:
        pass


@zope.interface.implementer(IAnimal)
class Cow(object):
    @overload
    def say(self) -> str:
        ...

    @overload
    def say(self, count: int) -> List[str]:
        ...

    def say(self, count: int = None) -> Union[str, List[str]]:
        if count is None:
            return "Mooo"
        return ["Mooo"] * count
```

### Type stubs for zope.interface and zope.schema

`mypy-zope` ships with type stubs (`*.pyi` files) for `zope.interface` and
`zope.schema` packages. They are enabled automatically as soon as plugin is
enabled.


## What is not supported?

These `zope.interface` features are not supported:

* Declaring modules as interface implementers.
* Type inference for `zope.schema.List` and `zope.schema.Dict` fields.
* Stub files are largely incomplete
* Interface compatibility checker will not type-check non-method attributes

## Ready to use!

Currently, the project is used in production in various substantially large
projects and considered production-grade, however there still might be subtle
bugs around. Suggestions and pull requests are welcomed!

