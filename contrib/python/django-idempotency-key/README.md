[![example workflow](https://github.com/yoyowallet/django-idempotency-key/actions/workflows/continuous_integration.yml/badge.svg?branch=master)](https://github.com/yoyowallet/django-idempotency-key/commit/master)
[![codecov.io](https://codecov.io/gh/yoyowallet/django-idempotency-key/branch/master/graphs/badge.svg?branch=master)](https://codecov.io/github/yoyowallet/django-idempotency-key)

## Idempotency key Django middleware
This package aims to solve the issue of a client creating multiple of the same object
because requests made to a server were lost in transit and the client cannot be sure
if the process was successful.

To solve this the client sends a key in the request header that uniquely identifies the
current operation so that if the server has already created object then it will not be
recreated.

This package allows API calls to opt-in or opt-out of using idempotency keys by default.
There is also an option that allows the client to choose to use idempotency keys if the
key is present in the header.

If the request has previously succeeded then the original data will be returned and
nothing new is created.

## Requirements

Django idempotency key requires the following:

Python (3.7 to 3.11)

Django (2.2, 3.2, 4.0, 4.1, 4.2)

The following is a list of Django versions and the respective
DjangoRestFramework and python versions that it supports.

Python  | Django | DRF
--------|--------|--------
 \>=3.7 | 2.2    | \>=3.8
 \>=3.7 | 3.2    | \>=3.11
 \>=3.8 | 4.0    | \>=3.13
 \>=3.8 | 4.1    | \>=3.14
 \>=3.8 | 4.2    | \>=3.14

## Installation

`pip install django_idempotency_key`

## Configuration

First, add to your MIDDLEWARE settings under your settings file.

If you want all non-safe view function to automatically use idempotency keys then use
the following:

```
MIDDLEWARE = [
   ...
   'idempotency_key.middleware.IdempotencyKeyMiddleware',
]
```

**WARNING** - Adding this as middleware will require that all client requests to
non-safe HTTP methods to supply an idempotency key specified in the request header
under HTTP_IDEMPOTENCY_KEY. If this is missing then a 400 BAD REQUEST is returned.

However, if you prefer that all view functions are exempt by default, and you will
opt in on a per view function basis then use the following:

```
MIDDLEWARE = [
   ...
   'idempotency_key.middleware.ExemptIdempotencyKeyMiddleware',
]
```

## Decorators
There are three decorators available that control how idempotency keys work with your
view function.

### `@idempotency_key(optional=False)`
This will ensure that the specified view function uses idempotency keys and will expect
the client to send the HTTP_IDEMPOTENCY_KEY (idempotency-key) header.

When `optional=True`, the idempotency key header can be optional. If the idempotency
key is missing, then the check will be skipped.

**NOTE:** If the IdempotencyKeyMiddleware class is used then this decorator
(with `optional=False`) is redundant.

### `@idempotency_key_exempt`
This will ensure that the specified view function is exempt from idempotency keys and
multiple requests with the same data will run the view function every time.

**NOTE:** If the ExemptIdempotencyKeyMiddleware class is used then this decorator is
redundant.

### `@idempotency_key_manual`
When specified the view function will dictate the response provided on a conflict.
The decorator will set two variables on the request object that informs the user if the
key exists in storage and what the response object was on the last call if the key
exists.

These two variables are defined as follows:

```
(boolean) request.idempotency_key_exists
(object) request.idempotency_key_response
```

`idempotency_key_response` will always return a Response object is set.

## Required header
When an idempotency key is enabled on a view function the calling client must specify a
unique key in the headers called HTTP_IDEMPOTENCY_KEY. This header name can be changed
to something else if required. If this is missing then a 400 BAD RESPONSE is returned.

## Settings
The following settings can be used to modify the behaviour of the idempotency key
middleware.
```
from idempotency_key import status

IDEMPOTENCY_KEY = {
    # Specify the key encoder class to be used for idempotency keys.
    # If not specified then defaults to 'idempotency_key.encoders.BasicKeyEncoder'
    'ENCODER_CLASS': 'idempotency_key.encoders.BasicKeyEncoder',

    # Set the response code on a conflict.
    # If not specified this defaults to HTTP_409_CONFLICT
    # If set to None then the original request's status code is used.
    'CONFLICT_STATUS_CODE': status.HTTP_409_CONFLICT,

    # Allows the idempotency key header sent from the client to be changed
    'HEADER': 'HTTP_IDEMPOTENCY_KEY',

    'STORAGE': {
        # Specify the storage class to be used for idempotency keys
        # If not specified then defaults to 'idempotency_key.storage.MemoryKeyStorage'
        'CLASS': 'idempotency_key.storage.MemoryKeyStorage',

        # Name of the django cache configuration to use for the CacheStorageKey storage
        # class.
        # This can be overriden using the @idempotency_key(cache_name='MyCacheName')
        # view/viewset function decorator.
        'CACHE_NAME': 'default',

        # When the response is to be stored you have the option of deciding when this
        # happens based on the responses status code. If the response status code
        # matches one of the statuses below then it will be stored.
        # The statuses below are the defaults used if this setting is not specified.
        'STORE_ON_STATUSES': [
            status.HTTP_200_OK,
            status.HTTP_201_CREATED,
            status.HTTP_202_ACCEPTED,
            status.HTTP_203_NON_AUTHORITATIVE_INFORMATION,
            status.HTTP_204_NO_CONTENT,
            status.HTTP_205_RESET_CONTENT,
            status.HTTP_206_PARTIAL_CONTENT,
            status.HTTP_207_MULTI_STATUS,
        ]
    },

    # The following settings deal with the process/thread lock that can be placed around the cache storage object
    # to ensure that multiple threads do not try to call the same view/viewset method at the same time.
    'LOCK': {
        # Specify the key object locking class to be used for locking access to the cache storage object.
        # If not specified then defaults to 'idempotency_key.locks.basic.ThreadLock'
        'CLASS': 'idempotency_key.locks.basic.ThreadLock',

        # Location of the Redis server if MultiProcessRedisLock is used otherwise this is ignored.
        # The host name can be specified or both the host name and the port separated by a colon ':'
        'LOCATION': 'localhost:6379',

        # The unique name to be used accross processes for the lock. Only used by the MultiProcessRedisLock class
        'NAME': 'MyLock',

        # The maximum time to live for the lock. If a lock is given and is never released this timeout forces the release
        # The lock time is in seconds and the default is None which means lock until it is manually released
        'TTL': None,

        # The use of a lock around the storage object so that only one thread at a time can access it.
        # By default this is set to true. WARNING: setting this to false may allow duplicate calls to occur if the timing
        # is right.
        'ENABLE': True,

        # If the ENABLE_LOCK setting is True above then this represents the timeout (in seconds as a floating point number)
        # to occur before the thread gives up waiting. If a timeout occurs the middleware will return a HTTP_423_LOCKED
        # response.
        'TIMEOUT': 0.1,
    },

}
```
