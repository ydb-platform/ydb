<a href="https://param.holoviz.org/">
    <img src="https://raw.githubusercontent.com/holoviz/param/main/doc/_static/logo_horizontal.png" width=250>
</a>

# Param

**Param** is a zero-dependency Python library that provides two main features:

- Easily create classes with **rich, declarative attributes** - `Parameter` objects - that include extended metadata for various purposes such as runtime type and range validation, documentation strings, default values or factories, nullability, etc. In this sense, Param is conceptually similar to libraries like Pydantic, Python's dataclasses, or Traitlets.
- A suite of expressive and composable APIs for **reactive programming**, enabling automatic updates on attribute changes, and declaring complex reactive dependencies and expressions that can be introspected by other frameworks to implement their own reactive workflows.

This combination of **rich attributes** and **reactive APIs** makes Param a solid foundation for constructing user interfaces, graphical applications, and responsive systems where data integrity and automatic synchronization are paramount. In fact, Param serves as the backbone of HoloViz’s [Panel](https://panel.holoviz.org) and [HoloViews](https://holoviews.org) libraries, powering their rich interactivity and data-driven workflows.

Here is a very simple example showing both features at play. We declare a UserForm class with three parameters: `age` as an *Integer* parameter and and `name` as a *String* parameter for user data, and `submit` as an *Event* parameter to simulate a button in a user interface. We also declare that the `save_user_to_db` method should be called automatically when the value of the `submit` attribute changes.

```python
import param

class UserForm(param.Parameterized):
    age = param.Integer(bounds=(0, None), doc='User age')
    name = param.String(doc='User name')
    submit = param.Event()

    @param.depends('submit', watch=True)
    def save_user_to_db(self):
        print(f'Saving user to db: name={self.name}, age={self.age}')
        ...

user = UserForm(name='Bob', age=25)

user.submit = True  # => Saving user to db: name=Bob, age=25
```

---

Enjoying Param? Show your support with a [Github star](https://github.com/holoviz/param) to help others discover it too! ⭐️

---

|    |    |
| --- | --- |
| Downloads | [![PyPi Downloads](https://img.shields.io/pypi/dm/param?label=pypi)](https://pypistats.org/packages/param)
| Build Status | [![Linux/MacOS/Windows Build Status](https://github.com/holoviz/param/workflows/tests/badge.svg)](https://github.com/holoviz/param/actions/workflows/test.yaml)
| Coverage | [![codecov](https://codecov.io/gh/holoviz/param/branch/main/graph/badge.svg)](https://codecov.io/gh/holoviz/param) |
| Latest dev release | [![Github tag](https://img.shields.io/github/v/tag/holoviz/param.svg?label=tag&colorB=11ccbb)](https://github.com/holoviz/param/tags) |
| Latest release | [![Github release](https://img.shields.io/github/release/holoviz/param.svg?label=tag&colorB=11ccbb)](https://github.com/holoviz/param/releases) [![PyPI version](https://img.shields.io/pypi/v/param.svg?colorB=cc77dd)](https://pypi.python.org/pypi/param) [![conda-forge version](https://img.shields.io/conda/v/conda-forge/param.svg?label=conda%7Cconda-forge&colorB=4488ff)](https://anaconda.org/conda-forge/param) [![defaults version](https://img.shields.io/conda/v/anaconda/param.svg?label=conda%7Cdefaults&style=flat&colorB=4488ff)](https://anaconda.org/anaconda/param) [![param version](https://img.shields.io/conda/v/pyviz/param.svg?colorB=4488ff&style=flat)](https://anaconda.org/pyviz/param) |
| Python | [![Python support](https://img.shields.io/pypi/pyversions/param.svg)](https://pypi.org/project/param/)
| Docs | [![gh-pages](https://img.shields.io/github/last-commit/holoviz/param/gh-pages.svg)](https://github.com/holoviz/param/tree/gh-pages) [![site](https://img.shields.io/website-up-down-green-red/https/param.holoviz.org.svg)](https://param.holoviz.org) |
| Binder | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/holoviz/param/main?labpath=doc) |
| Support | [![Discourse](https://img.shields.io/discourse/status?server=https%3A%2F%2Fdiscourse.holoviz.org)](https://discourse.holoviz.org/) |


## Rich class attributes for runtime validation and more

Param lets you create classes and declare facts about each of their attributes through rich `Parameter` objects. Once you have done that, Param can handle runtime attribute validation (type checking, range validation, etc.) and more (documentation, serialization, etc.). Let's see how to use `Parameter` objects with a simple example, a `Processor` class that has three attributes.

```python
import param

# Create a class by inheriting from Parameterized
class Processor(param.Parameterized):

    # An Integer parameter that allows only integer values between 0 and 10,
    # has a default of 3, and a custom docstring.
    retries = param.Integer(default=3, bounds=(0, 10), doc="Retry attempts.")

    # A Boolean parameter, False by default.
    verbose = param.Boolean(default=False, doc="Emit progress messages.")

    # A Selector parameter, with only two allowed values "fast" and "accurate",
    # "fast" by default.
    mode = param.Selector(
        default="fast", objects=["fast", "accurate"],
        doc="Execution strategy."
    )

    def run(self):
        if self.verbose:
            print(f"[{self.mode}] retry={self.retries}")
        # ...main logic...


# You can of course override the default value on instantiation.
processor = Processor(verbose=True)

# A Parameterized instance behaves as a usual instance of a class.
processor.run()
# => [fast] retry=3

# Parameterized implements a nice and simple repr.
print(repr(processor))
# => Processor(mode='accurate', name='Processor00042', retries=3, verbose=False)

# Attempting to set the retries attribute to 42 will raise
# an error as the value must be between 0 and 10.
try:
    processor.retries = 42
except ValueError as e:
    print(e)
    # => Integer parameter 'Processor.retries' must be at most 10, not 42.

# The `.param` namespace allows to get a hold on Parameter objects via
# indexing or attribute access.
print(processor.param['mode'].objects)  # => ['fast', 'accurate']
print(processor.param.mode.objects)  # => ['fast', 'accurate']

# This namespace offers many more useful methods, such as `.update()`
# to update multiple parameters at once, or `.values()` to obtain
# a dict of parameter name to parameter value.
processor.param.update(mode='accurate', verbose=False)
print(processor.param.values())
# => {'mode': 'accurate', 'name': 'Processor00042', 'retries': 3, 'verbose': False}
```

Runtime attribute validation is a great feature that helps build defendable code bases! Alternative libraries, like [Pydantic](https://docs.pydantic.dev) and others, excel at input validation, and if this is only what you need, you should probably look into them. Where Param shines is when you also need:

- Attributes that are also available at the class level, allowing to easily configure a hierarchy of classes and their instances.
- Parameters with rich metadata (`default`, `doc`, `label`, `bounds`, etc.) that downstream tooling can inspect to build configuration UIs, CLIs, or documentation automatically.
- Parameterized subclasses that inherit Parameter metadata from their parents, and can selectively override certain attributes (e.g. overriding `default` in a subclass).

Let's see this in action by extending the example above with a custom processor subclass:

```python
class CustomProcessor(Processor):
    # This subclass overrides the bounds of the `retries` Parameter
    # and the default of the `verbose` Parameter. All the other
    # Parameter metadata are inherited from Processor.
    retries = param.Integer(bounds=(0, 100))
    verbose = param.Boolean(default=True)


# Attributes exist at both the class-level and instance-level
print(CustomProcessor.verbose)  # => True

# The `.param` namespace is also available at the class-level.
# Parameter metadata inheritance in action, with `default`
# inherited from Processor and `bounds` overridden by CustomProcessor.
print(CustomProcessor.param['retries'].default)  # => 3
print(CustomProcessor.param['retries'].bounds)  # => (0, 100)

# Class attributes are also runtime validated.
try:
    CustomProcessor.retries = 200
except ValueError as e:
    print(e)
    # => Integer parameter 'CustomProcessor.retries' must be at most 100, not 200.

cprocessor = CustomProcessor()

# As in normal Python classes, class-level attribute values apply
# to all instances that didn't override it.
print(cprocessor.mode)  # => 'fast'
Processor.mode = 'accurate'
print(cprocessor.mode)  # => 'accurate'
```

## Reactive Programming

Param extends beyond rich class attributes with a suite of APIs for reactive programming. Let's do a quick tour!

We'll start with APIs that trigger side-effects only, which either have the noun watch in their name or are invoked with `watch=True`:

1. `<parameterized_obj>.param.watch(fn, *parameters, ...)`: Low-level, imperative API to attach callbacks to parameter changes, the callback receives one or more rich `Event` objects.
2. `@depends(*parameter_names, watch=True)`: In a Parameterized class, declare dependencies and automatically watch parameters for changes to call the decorated method.
3. `bind(fn, *references, watch=True, **kwargs)`: Function binding with automatic references (parameters, bound functions, reactive expressions) watching and triggering on changes.

```python
import param

def debug_event(event: param.parameterized.Event):
    print(event)

class SideEffectExample(param.Parameterized):
    a = param.String()
    b = param.String()
    c = param.String()

    def __init__(self, **params):
        super().__init__(**params)
        # We register the debug_event callback, that will be called when a changes.
        self.param.watch(debug_event, 'a')  # 1.

    # We declare an automatic dependency between b and the print_b method.
    @param.depends('b', watch=True)  # 2.
    def print_b(self):
        print(f"print_b: {self.b=}")

sfe = SideEffectExample()
# We update the value of a and immediately see that debug_event is called
# with a rich Event object.
sfe.a = 'foo'
# => Event(
#     what='value', name='a',
#     obj=SideEffectExample(a='foo', b='', c='', name='SideEffectExample00008'),
#     cls=SideEffectExample(a='foo', b='', c='', name='SideEffectExample00008'),
#     old='', new='foo', type='changed'
# )

# Updating b automatically calls print_b, which is simply invoked as is without
# any rich Event object passed. When called, b is already updated.
sfe.b = 'bar'
# print_b: self.b='bar'

def print_c(c):
    print(f"print_c: {c=}")

# We can also bind a function to a parameter.
param.bind(print_c, sfe.param.c, watch=True)  # 3.

# Updating c invokes the bound function with the updated value.
sfe.c = 'baz'
# print_c: c='baz'
```

Let's continue the tour with what we'll call "reactive APIs". Contrary to the APIs presented above, in this group parameter updates do not immediately trigger side effects. Instead, these APIs let you declare relationships, dependencies, and expressions, which can be introspected by other frameworks to set up their own reactive workflows.

1. `@depends(*parameter_names)`: In a Parameterized class, declare parameter dependencies by decorating a method.
2. `bind(fn, *references, **kwargs)`: Create a bound function, that when called, will always use the current parameter/reference value. `bind` is essentially a reactive version of [`functools.partial`](https://docs.python.org/3/library/functools.html#functools.partial).
3. `rx()`: Fluent API to create reactive expressions, which allow chaining and composing operations.

```python
import param

class ReactiveExample(param.Parameterized):
    x = param.Integer()
    y = param.Integer()

    @param.depends('x', 'y')  # 1.
    def sum(self):
        return self.x + self.y

re = ReactiveExample()

def mul(a, b):
    return a * b

bound_mul = param.bind(mul, re.param.x, re.param.y)  # 2.
re.param.update(x=2, y=4)
bound_mul()
# 8
```

`rx()` is the highest-level reactive API Param offers. We'll show a simple example first, using literal values as input of three source reactive expressions, that we combine with simple arithmetic operations.

```python
from param import rx

# Reactive expressions can be created by simply wrapping an object with `rx()`
val1 = rx(0)
val2 = rx(0)
factor = rx(1)

# We can then use these objects as if they were the object they wrap
# (a reactive expression is a proxy of its resolved value). Since the
# inputs are of type `int`, we are able to use the `+` and `*` operators
# to create a new reactive expression object `res`. This works by
# overloading the Python data model (`__add__` and `__mul__` in this
# example).
res = (val1 + val2) * factor
# We have built a small computational graph, adding first `val1` and
# `val2`, and then multiplying the sum with `factor`.

# We can obtain the resolved value of `res` via the `.rx.value` property
# (`.rx` being a namespace holding other useful attributes and methods).
print(res.rx.value)  # => 0, from (0 + 0) * 1

# We can then update one of the source reactive expressions by setting
# it via the `.rx.value` property.
val1.rx.value = 2
# Requesting the value of `res` resolves its updated value (2 instead of 0).
print(res.rx.value)  # => 2, from (2 + 0) * 1

# Similarly, we can update `factor` and request the new value of `res`.
# Note that only the last multiplication is recomputed; the intermediate
# result of `val1 + val2` is automatically cached.
factor.rx.value = 10
print(res.rx.value)  # => 20, from (2 + 0) * 10
```

The snippet below shows a slightly more complex example that reveals the power of reactive expressions.

```python
import param

class RXExample(param.Parameterized):
    val1 = param.String('foo')
    val2 = param.String('bar')

example = RXExample()

# Reactive expressions can be created directly from Parameters by
# calling `.rx()`. The source of the reactive expression is not a
# literal value, but a fully dynamic Parameter object.
print(example.param.val1.rx().title().rx.value)  # => 'Foo'

# By updating the parameter value, we update the source value of
# this expression.
example.val1 = 'fab'
print(example.param.val1.rx().title().rx.value)  # => 'Fab'

# Reactive expressions aren't constrained to one data type; the source
# value of `cond1` is a string and the resolved value a is boolean.
cond1 = example.param.val1.rx().startswith('o')
print(cond1.rx.value)  # => False

# Python does not allow all of its data model to be overloaded, and
# special methods like `.rx.or_` have been implemented to cover
# these cases.
cond2 = example.param.val2.rx().startswith('b')
print(cond1.rx.or_(cond2).rx.value)  # => True
```

A `Parameter` does not have to refer to a specific static value but can reference another object and update reactively when its value changes. We'll show a few examples of supported *references* (Parameters, bound functions, reactive expressions, etc.). Setting parameter values with references is an effective way to establish automatic one-way linking between a reference and a parameter (the reference being often driven by another parameter).

```python
import param

class X(param.Parameterized):

    # The source parameter is going to serve as the input of all
    # our references.
    source = param.Number()

class Y(param.Parameterized):

    # The target Parameter of this Y class is declared to accept references
    # with allow_refs=True (False by default).
    target = param.Number(allow_refs=True)

    # We add this automatic callback for you to better understand when the
    # updates actually occur; watch_target will be called when the resolved
    # value of target changes.
    @param.depends('target', watch=True)
    def watch_target(self):
        print(f'y.target updated to {self.target}')

x = X(source=1)

# The first example of a reference is simply another Parameter, here
# the source parameter of the x instance.
y = Y(target=x.param['source'])
# y.target is already equal to the value of x.source
print(y.target)  # => 1

# When x.source is updated, y.target is immediately and automatically updated.
x.source = 2
# y.target updated to 2
print(y.target)  # => 2

# We can override a reference with another reference, in this case a function
# bound to the x parameter of source.
y.target = param.bind(lambda x: x + 10, x.param['source'])
# y.target updated to 12
print(y.target)  # => 12

# When x.source is updated, the bound function is immediately called to update
# the value of y.target.
x.source = 3
# y.target updated to 13
print(y.target)  # => 13

# Another kind of accepted reference is a reactive expression.
y.target = x.param['source'].rx() * 20
# y.target updated to 60
print(y.target)  # => 60

# Similarly, when x.source is updated, the reactive expression is immediately
# resolved to update the value of y.target
x.source = 5
# y.target updated to 100
print(y.target)  # => 100
```

## Support & Feedback

- Visit [Param's website](https://param.holoviz.org/)
- Usage questions and showcases -> [HoloViz Community](https://holoviz.org/community.html)
- Bug reports and feature requests -> [Github](https://github.com/holoviz/param)
- Chat -> [Discord](https://discord.gg/rb6gPXbdAr)

For more detail check out the [HoloViz Community Guide](https://holoviz.org/community.html).


## Contributing

Check out the [Contributing Guide](CONTRIBUTING.MD).

## License

Param is completely free and open-source. It is licensed under the [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause).


## Sponsors

The Param project is also very grateful for the sponsorship by the organizations and companies below:

<table align="center">
<tr>
  <td>
    <a href="https://www.anaconda.com/">
      <img src="https://static.bokeh.org/sponsor/anaconda.png"
         alt="Anaconda Logo" width="200"/>
	 </a>
  </td>
  <td>
    <a href="https://numfocus.org/">
    <img src="https://numfocus.org/wp-content/uploads/2017/03/numfocusweblogo_orig-1.png"
         alt="NumFOCUS Logo" width="200"/>
    </a>
  </td>

</tr>
</table>
