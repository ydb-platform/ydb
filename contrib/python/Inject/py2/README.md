# python-inject [![Build Status](https://travis-ci.org/ivankorobkov/python-inject.svg?branch=master)](https://travis-ci.org/ivankorobkov/python-inject)
Dependency injection the python way, the good way. Not a port of Guice or Spring.

## Key features
* Fast.
* Thread-safe.
* Simple to use.
* Does not steal class constructors.
* Does not try to manage your application object graph.
* Transparently integrates into tests.
* Supports Python 2.7 and Python 3.3+.
* Supports type hinting in Python 3.5+.
* Autoparams leveraging type annotations.


## Installation
Use pip to install the lastest version:

```bash
pip install inject
```

## Autoparams example
`@inject.autoparams` returns a decorator which automatically injects arguments into a function 
that uses type annotations. This is supported only in Python >= 3.5.

```python
@inject.autoparams()
def refresh_cache(cache: RedisCache, db: DbInterface):
    pass
```

There is an option to specify which arguments we want to inject without attempts of 
injecting everything:

```python
@inject.autoparams('cache', 'db')
def sign_up(name, email, cache, db):
    pass
```

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
    binder.install(my_config2)  # Add bindings from another config.
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


## Usage with Django
Django can load some modules multiple times which can lead to 
`InjectorException: Injector is already configured`. You can use `configure_once` which
is guaranteed to run only once when the injector is absent:
```python
import inject
inject.configure_once(my_config)
```

## Testing
In tests use `inject.clear_and_configure(callable)` to create a new injector on setup,
and optionally `inject.clear()` to clean up on tear down:
```python
class MyTest(unittest.TestCase):
    def setUp(self):
        inject.clear_and_configure(lambda binder: binder
            .bind(Cache, Mock() \
            .bind(Validator, TestValidator())
    
    def tearDown(self):
        inject.clear()
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
explicitly bound instances are injected, pass `bind_in_runtime=False` 
to `inject.configure`, `inject.configure_once` or `inject.clear_and_configure`.

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

## Contributers
* Ivan Korobkov [@ivankorobkov](https://github.com/ivankorobkov)
* Jaime Wyant [@jaimewyant](https://github.com/jaimewyant)
* Sebastian Buczyński [@Enforcer](https://github.com/Enforcer)
* Oleksandr Fedorov [@Fedorof](https://github.com/Fedorof)
* cselvaraj [@cselvaraj](https://github.com/cselvaraj)
* 陆雨晴 [@SixExtreme](https://github.com/SixExtreme)
* Andrew William Borba [@andrewborba10](https://github.com/andrewborba10)
* jdmeyer3 [@jdmeyer3](https://github.com/jdmeyer3)
* Alex Grover [@ajgrover](https://github.com/ajgrover)
