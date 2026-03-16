[![Build Status](https://github.com/splunk/splunk-sdk-python/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/splunk/splunk-sdk-python/actions/workflows/test.yml)

[Reference Docs](https://dev.splunk.com/enterprise/reference)

# The Splunk Enterprise Software Development Kit for Python

#### Version 2.1.1

The Splunk Enterprise Software Development Kit (SDK) for Python contains library code designed to enable developers to build applications using the Splunk platform.

The Splunk platform is a search engine and analytic environment that uses a distributed map-reduce architecture to efficiently index, search, and process large time-varying data sets.

The Splunk platform is popular with system administrators for aggregation and monitoring of IT machine data, security, compliance, and a wide variety of other scenarios that share a requirement to efficiently index, search, analyze, and generate real-time notifications from large volumes of time-series data.

The Splunk developer platform enables developers to take advantage of the same technology used by the Splunk platform to build exciting new applications.

## Getting started with the Splunk SDK for Python


## Get started with the Splunk Enterprise SDK for Python

The Splunk Enterprise SDK for Python contains library code, and its examples are located in the [splunk-app-examples](https://github.com/splunk/splunk-app-examples) repository. They show how to programmatically interact with the Splunk platform for a variety of scenarios including searching, saved searches, data inputs, and many more, along with building complete applications.

### Requirements

Here's what you need to get going with the Splunk Enterprise SDK for Python.

* Python 3.7, Python 3.9 and Python 3.13
  
  The Splunk Enterprise SDK for Python is compatible with python3 and has been tested with Python v3.7, v3.9 and v3.13.

* Splunk Enterprise 9.2 or 8.2

    The Splunk Enterprise SDK for Python has been tested with Splunk Enterprise 9.2, 8.2 and 8.1

  If you haven't already installed Splunk Enterprise, download it [here](http://www.splunk.com/download). 
  For more information, see the Splunk Enterprise [_Installation Manual_](https://docs.splunk.com/Documentation/Splunk/latest/Installation).

* Splunk Enterprise SDK for Python

  Get the Splunk Enterprise SDK for Python from [PyPI](https://pypi.org/project/splunk-sdk/). If you want to contribute to the SDK, clone the repository from [GitHub](https://github.com/splunk/splunk-sdk-python).

### Install the SDK

Use the following commands to install the Splunk Enterprise SDK for Python libraries. However, it's not necessary to install the libraries to run the unit tests from the SDK.

Use `pip`:

    [sudo] pip install splunk-sdk

Install the Python egg:

    [sudo] pip install --egg splunk-sdk

Install the sources you cloned from GitHub:

    [sudo] python setup.py install

## Testing Quickstart

You'll need `docker` and `docker-compose` to get up and running using this method.

```
make up SPLUNK_VERSION=9.2
make wait_up
make test
make down
```

To run the examples and unit tests, you must put the root of the SDK on your PYTHONPATH. For example, if you downloaded the SDK to your home folder and are running OS X or Linux, add the following line to your **.bash_profile** file:

    export PYTHONPATH=~/splunk-sdk-python

### Following are the different ways to connect to Splunk Enterprise
#### Using username/password
```python
import splunklib.client as client
service = client.connect(host=<host_url>, username=<username>, password=<password>, autologin=True)
```

#### Using bearer token
```python
import splunklib.client as client
service = client.connect(host=<host_url>, splunkToken=<bearer_token>, autologin=True)
```

#### Using session key
```python
import splunklib.client as client
service = client.connect(host=<host_url>, token=<session_key>, autologin=True)
```

###
#### Update a .env file

To connect to Splunk Enterprise, many of the SDK examples and unit tests take command-line arguments that specify values for the host, port, and login credentials for Splunk Enterprise. For convenience during development, you can store these arguments as key-value pairs in a **.env** file. Then, the SDK examples and unit tests use the values from the **.env** file when you don't specify them.

>**Note**: Storing login credentials in the **.env** file is only for convenience during development. This file isn't part of the Splunk platform and shouldn't be used for storing user credentials for production. And, if you're at all concerned about the security of your credentials, enter them at the command line rather than saving them in this file.

here is an example of .env file:

    # Splunk Enterprise host (default: localhost)
    host=localhost
    # Splunk Enterprise admin port (default: 8089)
    port=8089
    # Splunk Enterprise username
    username=admin
    # Splunk Enterprise password
    password=changed!
    # Access scheme (default: https)
    scheme=https
    # Your version of Splunk Enterprise
    version=9.2
    # Bearer token for authentication
    #splunkToken=<Bearer-token>
    # Session key for authentication
    #token=<Session-Key>

#### SDK examples

Examples for the Splunk Enterprise SDK for Python are located in the [splunk-app-examples](https://github.com/splunk/splunk-app-examples) repository. For details, see the [Examples using the Splunk Enterprise SDK for Python](https://dev.splunk.com/enterprise/docs/devtools/python/sdk-python/examplespython) on the Splunk Developer Portal.

#### Run the unit tests

The Splunk Enterprise SDK for Python contains a collection of unit tests. To run them, open a command prompt in the **/splunk-sdk-python** directory and enter:

    make

You can also run individual test files, which are located in **/splunk-sdk-python/tests**. To run a specific test, enter:

    make test_specific

The test suite uses Python's standard library, the built-in `unittest` library, `pytest`, and `tox`.

>**Notes:**
>*  The test run fails unless the [SDK App Collection](https://github.com/splunk/sdk-app-collection) app is installed.
>*  To exclude app-specific tests, use the `make test_no_app` command.
>*  To learn about our testing framework, see [Splunk Test Suite](https://github.com/splunk/splunk-sdk-python/tree/master/tests) on GitHub.
>   In addition, the test run requires you to build the searchcommands app. The `make` command runs the tasks to do this, but more complex testing may require you to rebuild using the `make build_app` command.

## Repository

| Directory | Description                                                |
|:--------- |:---------------------------------------------------------- |
|/docs      | Source for Sphinx-based docs and build                     |
|/splunklib | Source for the Splunk library modules                      |
|/tests     | Source for unit tests                                      |
|/utils     | Source for utilities shared by the unit tests              |

### Customization
* When working with custom search commands such as Custom Streaming Commands or Custom Generating Commands, We may need to add new fields to the records based on certain conditions.
* Structural changes like this may not be preserved.
* Make sure to use ``add_field(record, fieldname, value)`` method from SearchCommand to add a new field and value to the record.
* ___Note:__ Usage of ``add_field`` method is completely optional, if you are not facing any issues with field retention._

Do
```python
class CustomStreamingCommand(StreamingCommand):
    def stream(self, records):
        for index, record in enumerate(records):
            if index % 1 == 0:
                self.add_field(record, "odd_record", "true")
            yield record
```

Don't
```python
class CustomStreamingCommand(StreamingCommand):
    def stream(self, records):
        for index, record in enumerate(records):
            if index % 1 == 0:
                record["odd_record"] = "true"
            yield record
```
### Customization for Generating Custom Search Command
* Generating Custom Search Command is used to generate events using SDK code.
* Make sure to use ``gen_record()`` method from SearchCommand to add a new record and pass event data as a key=value pair separated by , (mentioned in below example).

Do
```python
@Configuration()
class GeneratorTest(GeneratingCommand):
    def generate(self):
        yield self.gen_record(_time=time.time(), one=1)
        yield self.gen_record(_time=time.time(), two=2)
```

Don't
```python
@Configuration()
class GeneratorTest(GeneratingCommand):
    def generate(self):
        yield {'_time': time.time(), 'one': 1}
        yield {'_time': time.time(), 'two': 2}
```

### Access metadata of modular inputs app
* In stream_events() method we can access modular input app metadata from InputDefinition object
* See [GitHub Commit](https://github.com/splunk/splunk-app-examples/blob/master/modularinputs/python/github_commits/bin/github_commits.py) Modular input App example for reference.
```python
    def stream_events(self, inputs, ew):
        # other code
        
        # access metadata (like server_host, server_uri, etc) of modular inputs app from InputDefinition object
        # here inputs is a InputDefinition object
        server_host = inputs.metadata["server_host"]
        server_uri = inputs.metadata["server_uri"]
        
        # Get the checkpoint directory out of the modular input's metadata
        checkpoint_dir = inputs.metadata["checkpoint_dir"]
```

### Access service object in Custom Search Command & Modular Input apps

#### Custom Search Commands
* The service object is created from the Splunkd URI and session key passed to the command invocation the search results info file.
* Service object can be accessed using `self.service` in `generate`/`transform`/`stream`/`reduce` methods depending on the Custom Search Command.
* For Generating Custom Search Command
  ```python
    def generate(self):
        # other code
        
        # access service object that can be used to connect Splunk Service
        service = self.service
        # to get Splunk Service Info
        info = service.info
  ```

 

#### Modular Inputs app:
* The service object is created from the Splunkd URI and session key passed to the command invocation on the modular input stream respectively.
* It is available as soon as the `Script.stream_events` method is called.
```python
    def stream_events(self, inputs, ew):
        # other code
        
        # access service object that can be used to connect Splunk Service
        service = self.service
        # to get Splunk Service Info
        info = service.info
```


### Optional:Set up logging for splunklib
+ The default level is WARNING, which means that only events of this level and above will be visible
+ To change a logging level we can call setup_logging() method and pass the logging level as an argument.
+ Optional: we can also pass log format and date format string as a method argument to modify default format

```python
import logging
from splunklib import setup_logging

# To see debug and above level logs
setup_logging(logging.DEBUG)
```

### Changelog

The [CHANGELOG](CHANGELOG.md) contains a description of changes for each version of the SDK. For the latest version, see the [CHANGELOG.md](https://github.com/splunk/splunk-sdk-python/blob/master/CHANGELOG.md) on GitHub.

### Branches

The **master** branch represents a stable and released version of the SDK.
To learn about our branching model, see [Branching Model](https://github.com/splunk/splunk-sdk-python/wiki/Branching-Model) on GitHub.

## Documentation and resources

| Resource                | Description |
|:----------------------- |:----------- |
| [Splunk Developer Portal](http://dev.splunk.com) | General developer documentation, tools, and examples |
| [Integrate the Splunk platform using development tools for Python](https://dev.splunk.com/enterprise/docs/devtools/python)| Documentation for Python development |
| [Splunk Enterprise SDK for Python Reference](http://docs.splunk.com/Documentation/PythonSDK) | SDK API reference documentation |
| [REST API Reference Manual](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTprolog) | Splunk REST API reference documentation |
| [Splunk>Docs](https://docs.splunk.com/Documentation) | General documentation for the Splunk platform |
| [GitHub Wiki](https://github.com/splunk/splunk-sdk-python/wiki/) | Documentation for this SDK's repository on GitHub |
| [Splunk Enterprise SDK for Python Examples](https://github.com/splunk/splunk-app-examples) | Examples for this SDK's repository |

## Community

Stay connected with other developers building on the Splunk platform.

* [Email](mailto:devinfo@splunk.com)
* [Issues and pull requests](https://github.com/splunk/splunk-sdk-python/issues/)
* [Community Slack](https://splunk-usergroups.slack.com/app_redirect?channel=appdev)
* [Splunk Answers](https://community.splunk.com/t5/Splunk-Development/ct-p/developer-tools)
* [Splunk Blogs](https://www.splunk.com/blog)
* [Twitter](https://twitter.com/splunkdev)

### Contributions

If you would like to contribute to the SDK, see [Contributing to Splunk](https://www.splunk.com/en_us/form/contributions.html). For additional guidelines, see [CONTRIBUTING](CONTRIBUTING.md). 

### Support

*  You will be granted support if you or your company are already covered under an existing maintenance/support agreement. Submit a new case in the [Support Portal](https://www.splunk.com/en_us/support-and-services.html) and include "Splunk Enterprise SDK for Python" in the subject line.

   If you are not covered under an existing maintenance/support agreement, you can find help through the broader community at [Splunk Answers](https://community.splunk.com/t5/Splunk-Development/ct-p/developer-tools).

*  Splunk will NOT provide support for SDKs if the core library (the code in the <b>/splunklib</b> directory) has been modified. If you modify an SDK and want support, you can find help through the broader community and [Splunk Answers](https://community.splunk.com/t5/Splunk-Development/ct-p/developer-tools). 

   We would also like to know why you modified the core library, so please send feedback to _devinfo@splunk.com_.

*  File any issues on [GitHub](https://github.com/splunk/splunk-sdk-python/issues).

### Contact Us

You can reach the Splunk Developer Platform team at _devinfo@splunk.com_.

## License

The Splunk Enterprise Software Development Kit for Python is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
