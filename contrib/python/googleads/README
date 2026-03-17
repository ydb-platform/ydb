# The Google Ad Manager SOAP API Python client library

This client library simplifies accessing the Google Ad Manager SOAP API.
The library provides easy ways to store your authentication and
create SOAP web service clients. It also contains example code to help you get
started integrating with our APIs.

## Getting started
1. Download and install the library

   *[setuptools](https://pypi.python.org/pypi/setuptools) is a pre-requisite
   for installing the googleads library*

   It is recommended that you install the library and its dependencies from
   PyPI using [pip](https://pip.pypa.io/en/stable/installing/). This can be
   accomplished with a single command:

   `$ pip install googleads`

   As an alternative, you can
   [download the library as a tarball](https://pypi.python.org/pypi/googleads).
   To start the installation, navigate to the directory that contains your
   downloaded unzipped client library and run the "setup.py" script as follows:

   `$ python setup.py build install`

2. Copy the [googleads.yaml](https://github.com/googleads/googleads-python-lib/blob/main/googleads.yaml)
   file to your home directory.

   This will be used to store credentials and other settings that can be loaded
   to initialize a client.

3. Set up your OAuth2 credentials

   The Ad Manager API uses [OAuth2](http://oauth.net/2/) as the authentication
4. mechanism. Follow the appropriate guide below based on your use case.

   **If you're accessing an API using your own credentials...**

   * [Using Ad Manager](https://github.com/googleads/googleads-python-lib/wiki/API-access-using-own-credentials-(server-to-server-flow))

   **If you're accessing an API on behalf of clients...**

   * [Developing a web application (Ad Manager)](https://github.com/googleads/googleads-python-lib/wiki/API-access-on-behalf-of-your-clients-(web-flow))

#### Where can I find samples?

You can find code examples for the latest versions of Ad Manager on the
[releases](https://github.com/googleads/googleads-python-lib/releases) page.

Alternatively, you can find
[Ad Manager](https://github.com/googleads/googleads-python-lib/tree/main/examples/ad_manager)
samples in the examples directory of this repository.

#### Caching authentication information

It is possible to cache your API authentication information. The library
includes a sample file showing how to do this named `googleads.yaml`. Fill
in the fields for the API and features you plan to use. The library's
`LoadFromStorage` methods default to looking for a file with this name in your
home directory, but you can pass in a path to any file with the correct yaml
contents.

```python
# Use the default location - your home directory:
ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage()

# Alternatively, pass in the location of the file:
ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage('C:\My\Directory\googleads.yaml')
```

## Where do I submit bug reports and/or feature requests?

If you have issues directly related to the client library, use the [issue
tracker](https://github.com/googleads/googleads-python-lib/issues).


If you have issues pertaining to a specific API, use the product support forums:

* [Ad Manager](https://groups.google.com/forum/#!forum/google-doubleclick-for-publishers-api)


## How do I log SOAP interactions?
The library uses Python's built in logging framework. You can specify your
configuration via the configuration file; see [googleads.yaml](https://github.com/googleads/googleads-python-lib/blob/main/googleads.yaml)
for an example.

Alternatively, you can manually specify your logging configuration. For example,
if you want to log your SOAP interactions to stdout, and you are using the Zeep soap library, you
can do the following:
```python
logging.basicConfig(level=logging.INFO, format=googleads.util.LOGGER_FORMAT)
logging.getLogger('googleads.soap').setLevel(logging.DEBUG)
```
If you wish to log to a file, you'll need to attach a log handler to this source
which is configured to write the output to a file.


## How do I disable log filters?
The zeep plugin used for logging strips sensitive data from its output. If you would like this data
included in logs, you'll need to implement your own simple logging plugin. For example:
```python
class DangerousZeepLogger(zeep.Plugin):
  def ingress(self, envelope, http_headers, operation):
    logging.debug('Incoming response: \n%s', etree.tostring(envelope, pretty_print=True))
    return envelope, http_headers

  def egress(self, envelope, http_headers, operation, binding_options):
    logging.debug('Incoming response: \n%s', etree.tostring(envelope, pretty_print=True))
    return envelope, http_headers

ad_manager_client.zeep_client.plugins.append(DangerousZeepLogger())
```

## How can I configure or disable caching?

By default, clients are cached because reading and digesting the WSDL
can be expensive. However, the default caching method requires permission to
access a local file system that may not be available in certain hosting
environments such as App Engine.

You can pass an implementation of `zeep.cache.Base` to the `AdManagerClient`
initializer to modify the default caching behavior.

For example, configuring a different location and duration of the cache file
with zeep
```python
doc_cache = zeep.cache.SqliteCache(path=cache_path)
ad_manager_client = ad_manager.AdManagerClient(
  oauth2_client, application_name, network_code=network_code, cache=doc_cache)
```

You can also disable caching in similar fashion with zeep
```python
ad_manager_client = ad_manager.AdManagerClient(
  oauth2_client, application_name, network_code=network_code,
  cache=googleads.common.ZeepServiceProxy.NO_CACHE)
```

## Requirements

### Python Versions

This library only supports Python 3.7+.

### External Dependencies:

    - httplib2             -- https://pypi.python.org/pypi/httplib2/
    - oauth2client         -- https://pypi.python.org/pypi/oauth2client/
    - pysocks              -- https://pypi.python.org/pypi/PySocks/
    - pytz                 -- https://pypi.python.org/pypi/pytz
    - pyYAML               -- https://pypi.python.org/pypi/pyYAML/
    - xmltodict            -- https://pypi.python.org/pypi/xmltodict/
    - zeep                 -- https://pypi.python.org/pypi/zeep
    - mock                 -- https://pypi.python.org/pypi/mock
                              (only needed to run unit tests)
    - pyfakefs             -- https://pypi.python.org/pypi/pyfakefs
                              (only needed to run unit tests)


## Authors:
    Mark Saniscalchi
    David Wihl
    Ben Karl
