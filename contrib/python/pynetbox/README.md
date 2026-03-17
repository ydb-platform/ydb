# Pynetbox
Python API client library for [NetBox](https://github.com/netbox-community/netbox).

> **Note:** Version 6.7 and later of the library only supports NetBox 3.3 and above.

## Compatibility

Each pyNetBox Version listed below has been tested with its corresponding NetBox Version.

| NetBox Version | Plugin Version |
|:--------------:|:--------------:|
|      4.5       |     7.6.1      |
|      4.5       |     7.6.0      |
|      4.4       |     7.5.0      |
|      4.3       |     7.5.0      |
|      4.2       |     7.5.0      |
|      4.1       |     7.5.0      |
|      4.0.6     |     7.4.1      |
|      4.0.0     |     7.3.4      |
|      3.7       |     7.3.0      |
|      3.6       |     7.2.0      |
|      3.5       |     7.1.0      |
|      3.3       |     7.0.0      |

## Installation

To install run `pip install pynetbox`.

Alternatively, you can clone the repo and run `python setup.py install`.


## Quick Start

The full pynetbox API is documented on [GitHub Pages](https://netbox-community.github.io/pynetbox/), but the following should be enough to get started using it.

To begin, import pynetbox and instantiate the API.

```
import pynetbox
nb = pynetbox.api(
    'http://localhost:8000',
    token='d6f4e314a5b5fefd164995169f28ae32d987704f'
)
```

The first argument the .api() method takes is the NetBox URL. There are a handful of named arguments you can provide, but in most cases none are required to simply pull data. In order to write, the `token` argument should to be provided.


## Queries

The pynetbox API is setup so that NetBox's apps are attributes of the `.api()` object, and in turn those apps have attribute representing each endpoint. Each endpoint has a handful of methods available to carry out actions on the endpoint. For example, in order to query all the objects in the `devices` endpoint you would do the following:

```
>>> devices = nb.dcim.devices.all()
>>> for device in devices:
...     print(device.name)
...
test1-leaf1
test1-leaf2
test1-leaf3
>>>
```

Note that the all() and filter() methods are generators and return an object that can be iterated over only once.  If you are going to be iterating over it repeatedly you need to either call the all() method again, or encapsulate the results in a `list` object like this:
```
>>> devices = list(nb.dcim.devices.all())
```

### Threading

pynetbox supports multithreaded calls for `.filter()` and `.all()` queries. It is **highly recommended** you have `MAX_PAGE_SIZE` in your Netbox install set to anything *except* `0` or `None`. The default value of `1000` is usually a good value to use. To enable threading, add `threading=True` parameter to the `.api`:

```python
nb = pynetbox.api(
    'http://localhost:8000',
    threading=True,
)
```
### Filters validation

NetBox doesn't validate filters passed to the GET API endpoints, which are accessed with `.get()` and `.filter()`. If a filter is incorrect, NetBox silently returns the entire database table content. Pynetbox allows to check provided parameters against NetBox OpenAPI specification before doing the call, and raise an exception if a parameter is incorrect.

This can be enabled globally by setting `strict_filters=True` in the API object initialization:

```python
nb = pynetbox.api(
    'http://localhost:8000',
    strict_filters=True,
)
```

This can also be enabled and disabled on a per-request basis:

```python
# Disable for one request when enabled globally.
# Will not raise an exception and return the entire Device table.
nb.dcim.devices.filter(non_existing_filter="aaaa", strict_filters=False)

# Enable for one request when not enabled globally.
# Will raise an exception.
nb.dcim.devices.filter(non_existing_filter="aaaa", strict_filters=True)
```

## Running Tests

First, create and activate a Python virtual environment in the pynetbox directory to isolate the project dependencies:

```python
python3 -m venv venv
source venv/bin/activate
```

Install both requirements files:

```python
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

The test suite requires Docker to be installed and running, as it will download and launch netbox-docker containers during test execution.

With Docker installed and running, execute the following command to run the test suite:

```python
pytest
```
