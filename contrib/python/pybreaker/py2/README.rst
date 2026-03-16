
PyBreaker
=========

.. image:: https://travis-ci.org/danielfm/pybreaker.svg?branch=master
    :target: https://travis-ci.org/danielfm/pybreaker

PyBreaker is a Python implementation of the Circuit Breaker pattern, described
in Michael T. Nygard's book `Release It!`_.

In Nygard's words, *"circuit breakers exists to allow one subsystem to fail
without destroying the entire system. This is done by wrapping dangerous
operations (typically integration points) with a component that can circumvent
calls when the system is not healthy"*.


Features
--------

* Configurable list of excluded exceptions (e.g. business exceptions)
* Configurable failure threshold and reset timeout
* Support for several event listeners per circuit breaker
* Can guard generator functions
* Functions and properties for easy monitoring and management
* Thread-safe
* Optional redis backing
* Optional support for asynchronous Tornado calls


Requirements
------------

* `Python`_ 2.7+ (or Python 3.0+)


Installation
------------

Run the following command line to download the latest stable version of
PyBreaker from `PyPI`_::

    $ easy_install -U pybreaker

If you are a `Git`_ user, you might want to download the current development
version::

    $ git clone git://github.com/danielfm/pybreaker.git
    $ cd pybreaker
    $ python setup.py test
    $ python setup.py install


Usage
-----

The first step is to create an instance of ``CircuitBreaker`` for each
integration point you want to protect against::

    import pybreaker

    # Used in database integration points
    db_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=60)


``CircuitBreaker`` instances should live globally inside the application scope,
e.g., live across requests.

.. note::

  Integration points to external services (i.e. databases, queues, etc) are
  more likely to fail, so make sure to always use timeouts when accessing such
  services if there's support at the API level.

If you'd like to use the Redis backing, initialize the ``CircuitBreaker`` with a ``CircuitRedisStorage``::

    import pybreaker
    import redis

    redis = redis.StrictRedis()
    db_breaker = pybreaker.CircuitBreaker(
        fail_max=5,
        reset_timeout=60,
        state_storage=pybreaker.CircuitRedisStorage(pybreaker.STATE_CLOSED, redis))

.. note::

  You may want to reuse a connection already created in your application, if you're using ``django_redis`` for example::

    import pybreaker
    from django_redis import get_redis_connection

    db_breaker = pybreaker.CircuitBreaker(
        fail_max=5,
        reset_timeout=60,
        state_storage=pybreaker.CircuitRedisStorage(pybreaker.STATE_CLOSED, get_redis_connection('default')))


Event Listening
```````````````

There's no need to subclass ``CircuitBreaker`` if you just want to take action
when certain events occur. In that case, it's better to subclass
``CircuitBreakerListener`` instead::

    class DBListener(pybreaker.CircuitBreakerListener):
        "Listener used by circuit breakers that execute database operations."

        def before_call(self, cb, func, *args, **kwargs):
            "Called before the circuit breaker `cb` calls `func`."
            pass

        def state_change(self, cb, old_state, new_state):
            "Called when the circuit breaker `cb` state changes."
            pass

        def failure(self, cb, exc):
            "Called when a function invocation raises a system error."
            pass

        def success(self, cb):
            "Called when a function invocation succeeds."
            pass

    class LogListener(pybreaker.CircuitBreakerListener):
        "Listener used to log circuit breaker events."

        def state_change(self, cb, old_state, new_state):
            msg = "State Change: CB: {0}, New State: {1}".format(cb.name, new_state)
            logging.info(msg)


To add listeners to a circuit breaker::

    # At creation time...
    db_breaker = pybreaker.CircuitBreaker(listeners=[DBListener(), LogListener()])

    # ...or later
    db_breaker.add_listeners(OneListener(), AnotherListener())


What Does a Circuit Breaker Do?
```````````````````````````````

Let's say you want to use a circuit breaker on a function that updates a row
in the ``customer`` database table::

    @db_breaker
    def update_customer(cust):
        # Do stuff here...
        pass

    # Will trigger the circuit breaker
    updated_customer = update_customer(my_customer)


Or if you don't want to use the decorator syntax::

    def update_customer(cust):
        # Do stuff here...
        pass

    # Will trigger the circuit breaker
    updated_customer = db_breaker.call(update_customer, my_customer)


According to the default parameters, the circuit breaker ``db_breaker`` will
automatically open the circuit after 5 consecutive failures in
``update_customer``.

When the circuit is open, all calls to ``update_customer`` will fail immediately
(raising ``CircuitBreakerError``) without any attempt to execute the real
operation.

After 60 seconds, the circuit breaker will allow the next call to
``update_customer`` pass through. If that call succeeds, the circuit is closed;
if it fails, however, the circuit is opened again until another timeout elapses.

Optional Tornado Support
```````````````````````````````
A circuit breaker can (optionally) be used to call asynchronous Tornado functions::

    from tornado import gen

    @db_breaker(__pybreaker_call_async=True)
    @gen.coroutine
    def async_update(cust):
        # Do async stuff here...
        pass

Or if you don't want to use the decorator syntax::

    @gen.coroutine
    def async_update(cust):
        # Do async stuff here...
        pass

    updated_customer = db_breaker.call_async(async_update, my_customer)


Excluding Exceptions
````````````````````

By default, a failed call is any call that raises an exception. However, it's
common to raise exceptions to also indicate business exceptions, and those
exceptions should be ignored by the circuit breaker as they don't indicate
system errors::

    # At creation time...
    db_breaker = CircuitBreaker(exclude=[CustomerValidationError])

    # ...or later
    db_breaker.add_excluded_exception(CustomerValidationError)


In that case, when any function guarded by that circuit breaker raises
``CustomerValidationError`` (or any exception derived from
``CustomerValidationError``), that call won't be considered a system failure.

So as to cover cases where the exception class alone is not enough to determine
whether it represents a system error, you may also pass a callable rather than
a type::

    db_breaker = CircuitBreaker(exclude=[lambda e: type(e) == HTTPError and e.status_code < 500])

You may mix types and filter callables freely.


Monitoring and Management
`````````````````````````

A circuit breaker provides properties and functions you can use to monitor and
change its current state::

    # Get the current number of consecutive failures
    print db_breaker.fail_counter

    # Get/set the maximum number of consecutive failures
    print db_breaker.fail_max
    db_breaker.fail_max = 10

    # Get/set the current reset timeout period (in seconds)
    print db_breaker.reset_timeout
    db_breaker.reset_timeout = 60

    # Get the current state, i.e., 'open', 'half-open', 'closed'
    print db_breaker.current_state

    # Closes the circuit
    db_breaker.close()

    # Half-opens the circuit
    db_breaker.half_open()

    # Opens the circuit
    db_breaker.open()


These properties and functions might and should be exposed to the operations
staff somehow as they help them to detect problems in the system.


Donate
------

If this project is useful for you, buy me a beer!

Bitcoin: ``bc1qtwyfcj7pssk0krn5wyfaca47caar6nk9yyc4mu``

.. _Python: http://python.org
.. _Jython: http://jython.org
.. _Release It!: http://pragprog.com/titles/mnee/release-it
.. _PyPI: http://pypi.python.org
.. _Git: http://git-scm.com
