# python-inject [![Build Status](https://travis-ci.org/ivankorobkov/python-inject.svg?branch=master)](https://travis-ci.org/ivankorobkov/python-inject)
Dependency injection the python way, the good way.

## Key features
* Fast.
* Thread-safe.
* Simple to use.
* Does not steal class constructors.
* Does not try to manage your application object graph.
* Transparently integrates into tests.
* Autoparams leveraging type annotations.
* Supports type hinting in Python 3.5+.
* Supports Python 3.9+ (`v5.*`), 3.5-3.8 (`v4.*`) and Python 2.7–3.5 (`v3.*`).
* Supports context managers.

## Python Support

| Python  | Inject Version |
|---------|----------------|
| 3.9+    | 5.0+           |
| 3.6-3.8 | 4.1+, < 5.0    |
| 3.5     | 4.0            |
| < 3.5   | 3.*            |


## Installation
Use pip to install the lastest version:

```bash
pip install inject
```

## Autoparams example
`@inject.autoparams` returns a decorator which automatically injects arguments into a function 
that uses type annotations. This is supported only in Python >= 3.5.

```python
@inject.autoparams
def refresh_cache(cache: RedisCache, db: DbInterface):
    pass
```

There is an option to specify which arguments we want to inject without attempts of 
injecting everything:

```python
@inject.autoparams('cache', 'db')
def sign_up(name, email, cache: RedisCache, db: DbInterface):
    pass
```

It is also acceptable to use explicit curly braces notation (`@inject.autoparams()`)
for non-parameterized decorations — it will be treated the same as `@inject.autoparams`.

## Step-by-step example
```python
# Import the inject module.
import inject


# `inject.instance` requests dependencies from the injector.
def foo(bar):
    cache = inject.instance(Cache)
    cache.save('bar', bar)


# `inject.params` injects dependencies as keyword arguments or positional argument. 
# Also you can use @inject.autoparams in Python 3.5, see the example above.
@inject.params(cache=Cache, user=CurrentUser)
def baz(foo, cache=None, user=None):
    cache.save('foo', foo, user)

# this can be called in different ways:
# with injected arguments
baz('foo')

# with positional arguments
baz('foo', my_cache)

# with keyword arguments
baz('foo', my_cache, user=current_user)


# `inject.param` is deprecated, use `inject.params` instead.
@inject.param('cache', Cache)
def bar(foo, cache=None):
    cache.save('foo', foo)


# `inject.attr` creates properties (descriptors) which request dependencies on access.
class User(object):
    cache = inject.attr(Cache)
            
    def __init__(self, id):
        self.id = id

    def save(self):
        self.cache.save('users', self)
    
    @classmethod
    def load(cls, id):
        return cls.cache.load('users', id)


# Create an optional configuration.
def my_config(binder):
    binder.bind(Cache, RedisCache('localhost:1234'))

# Configure a shared injector.
inject.configure(my_config)


# Instantiate User as a normal class. Its `cache` dependency is injected when accessed.
user = User(10)
user.save()

# Call the functions, the dependencies are automatically injected.
foo('Hello')
bar('world')
```


## Context managers
Binding a class to an instance of a context manager (through `bind` or `bind_to_constructor`) 
or to a function decorated as a context manager leads to the context manager to be used as is, 
not via with statement.

```python
@contextlib.contextmanager
def get_file_sync():
    obj = MockFile()
    yield obj
    obj.destroy()

@contextlib.asynccontextmanager
async def get_conn_async():
    obj = MockConnection()
    yield obj
    obj.destroy()

def config(binder):
    binder.bind_to_provider(MockFile, get_file_sync)
    binder.bind(int, 100)
    binder.bind_to_provider(str, lambda: "Hello")
    binder.bind_to_provider(MockConnection, get_conn_sync)

    inject.configure(config)

@inject.autoparams()
def example(conn: MockConnection, file: MockFile):
    # Connection and file will be automatically destroyed on exit.
    pass
```


## Usage with Django
Django can load some modules multiple times which can lead to 
`InjectorException: Injector is already configured`. You can use `configure(once=True)` which
is guaranteed to run only once when the injector is absent:
```python
import inject
inject.configure(my_config, once=True)
```

## Testing
In tests use `inject.configure(callable, clear=True)` to create a new injector on setup,
and optionally `inject.clear()` to clean up on tear down:
```python
class MyTest(unittest.TestCase):
    def setUp(self):
        inject.configure(lambda binder: binder
            .bind(Cache, MockCache()) \
            .bind(Validator, TestValidator()),
            clear=True)
    
    def tearDown(self):
        inject.clear()
```

## Composable configurations
You can reuse configurations and override already registered dependencies to fit the needs 
in different environments or specific tests.
```python
    def base_config(binder):
        # ... more dependencies registered here
        binder.bind(Validator, RealValidator())
        binder.bind(Cache, RedisCache('localhost:1234'))

    def tests_config(binder):
        # reuse existing configuration
        binder.install(base_config)

        # override only certain dependencies
        binder.bind(Validator, TestValidator())
        binder.bind(Cache, MockCache())
    
    inject.configure(tests_config, allow_override=True, clear=True)
        
```

## Thread-safety
After configuration the injector is thread-safe and can be safely reused by multiple threads.

## Binding types
**Instance** bindings always return the same instance:

```python
redis = RedisCache(address='localhost:1234')
def config(binder):
    binder.bind(Cache, redis)
```
    
**Constructor** bindings create a singleton on injection:

```python
def config(binder):
    # Creates a redis cache singleton on first injection.
    binder.bind_to_constructor(Cache, lambda: RedisCache(address='localhost:1234'))
```

**Provider** bindings call the provider on injection:

```python
def get_my_thread_local_cache():
    pass

def config(binder):
    # Executes the provider on each injection.
    binder.bind_to_provider(Cache, get_my_thread_local_cache) 
```

**Runtime** bindings automatically create singletons on injection, require no configuration.
For example, only the `Config` class binding is present, other bindings are runtime:

```python
class Config(object):
    pass

class Cache(object):
    config = inject.attr(Config)

class Db(object):
    config = inject.attr(Config)

class User(object):
    cache = inject.attr(Cache)
    db = inject.attr(Db)
    
    @classmethod
    def load(cls, user_id):
        return cls.cache.load('users', user_id) or cls.db.load('users', user_id)
    
inject.configure(lambda binder: binder.bind(Config, load_config_file()))
user = User.load(10)
```
## Disabling runtime binding
Sometimes runtime binding leads to unexpected behaviour.  Say if you forget
to bind an instance to a class, `inject` will try to implicitly instantiate it.

If an instance is unintentionally created with default arguments it may lead to
hard-to-debug bugs.  To disable runtime binding and make sure that only 
explicitly bound instances are injected, pass `bind_in_runtime=False` to `inject.configure`.

In this case `inject` immediately raises `InjectorException` when the code
tries to get an unbound instance.

## Keys
It is possible to use any hashable object as a binding key. For example:

```python
import inject

inject.configure(lambda binder: \
    binder.bind('host', 'localhost') \
    binder.bind('port', 1234))
```

## Why no scopes?
I've used Guice and Spring in Java for a lot of years, and I don't like their scopes.
`python-inject` by default creates objects as singletons. It does not need a prototype scope
as in Spring or NO_SCOPE as in Guice because `python-inject` does not steal your class 
constructors. Create instances the way you like and then inject dependencies into them.

Other scopes such as a request scope or a session scope are fragile, introduce high coupling,
and are difficult to test. In `python-inject` write custom providers which can be thread-local, 
request-local, etc.

For example, a thread-local current user provider:

```python
import inject
import threading

# Given a user class.
class User(object):
    pass

# Create a thread-local current user storage.
_LOCAL = threading.local()

def get_current_user():
    return getattr(_LOCAL, 'user', None)

def set_current_user(user):
    _LOCAL.user = user

# Bind User to a custom provider.
inject.configure(lambda binder: binder.bind_to_provider(User, get_current_user))

# Inject the current user.
@inject.params(user=User)
def foo(user):
    pass
```

## Links
* Project: https://github.com/ivankorobkov/python-inject

## License
Apache License 2.0

## Contributors
* Ivan Korobkov [@ivankorobkov](https://github.com/ivankorobkov)
* Jaime Wyant [@jaimewyant](https://github.com/jaimewyant)
* Sebastian Buczyński [@Enforcer](https://github.com/Enforcer)
* Oleksandr Fedorov [@Fedorof](https://github.com/Fedorof)
* cselvaraj [@cselvaraj](https://github.com/cselvaraj)
* 陆雨晴 [@SixExtreme](https://github.com/SixExtreme)
* Andrew William Borba [@andrewborba10](https://github.com/andrewborba10)
* jdmeyer3 [@jdmeyer3](https://github.com/jdmeyer3)
* Alex Grover [@ajgrover](https://github.com/ajgrover)
* Harro van der Kroft [@wisepotato](https://github.com/wisepotato)
* Samiur Rahman [@samiur](https://github.com/samiur)
* 45deg [@45deg](https://github.com/45deg)
* Alexander Nicholas Costas [@ancostas](https://github.com/ancostas)
* Dmitry Balabka [@dbalabka](https://github.com/dbalabka)
* Dima Burmistrov [@pyctrl](https://github.com/pyctrl)
