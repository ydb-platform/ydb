# Toloka-Kit

[![License](https://img.shields.io/pypi/l/toloka-kit.svg)](https://github.com/toloka/toloka-kit/blob/master/LICENSE)
[![PyPI Latest Release](https://img.shields.io/pypi/v/toloka-kit.svg)](https://pypi.org/project/toloka-kit/)
[![Supported Versions](https://img.shields.io/pypi/pyversions/toloka-kit.svg)](https://pypi.org/project/toloka-kit)
[![Downloads](https://pepy.tech/badge/toloka-kit/month)](https://pepy.tech/project/toloka-kit)

[![Coverage](https://codecov.io/gh/Toloka/toloka-kit/branch/main/graph/badge.svg)](https://codecov.io/gh/Toloka/toloka-kit)
[![GitHub Tests](https://github.com/Toloka/toloka-kit/workflows/Tests/badge.svg?branch=main)](//github.com/Toloka/toloka-kit/actions?query=workflow:Tests)

[Toloka website](https://toloka.ai/?utm_source=github&utm_medium=site&utm_campaign=tolokakit) | [Documentation](https://toloka.ai/docs/toloka-kit/?utm_source=github&utm_medium=site&utm_campaign=tolokakit) | [Issue tracker](https://github.com/Toloka/toloka-kit/issues)



Toloka-Kit is a Python library for working with [Toloka API](https://toloka.ai/docs/api/api-reference/?utm_source=github&utm_medium=site&utm_campaign=tolokakit).

The API allows you to build scalable and fully automated human-in-the-loop ML pipelines, and integrate them into your processes. The toolkit makes integration easier. You can use it with Jupyter notebooks.

* Support for all common Toloka use cases: creating projects, adding pools, uploading tasks, and so on.
* Toloka entities are represented as Python classes. You can use them instead of accessing the API using JSON representations.
* There’s no need to validate JSON files and work with them directly.
* Support of both synchronous and asynchronous (via async/await) executions.
* Streaming support: build complex pipelines which send and receive data in real time. For example, you can [pass data between two related projects](https://github.com/Toloka/toloka-kit/blob/main/examples/6.streaming_pipelines/streaming_pipelines.ipynb): one for data labeling, and another for its validation.
* [AutoQuality](https://medium.com/toloka/automating-crowdsourcing-quality-control-ad057baf00fd) feature which automatically finds the best fitting quality control rules for your project.

## Prerequisites

Before you begin, make sure that:
* You are using [Python](https://www.python.org/) v3.8 or higher.
* You are [registered](https://toloka.ai/docs/guide/access/?utm_source=github&utm_medium=site&utm_campaign=tolokakit) in Toloka as a requester.
* You have [topped up](https://toloka.ai/docs/guide/refill/?utm_source=github&utm_medium=site&utm_campaign=tolokakit) your Toloka account.
* You have [set up an OAuth token](https://toloka.ai/docs/api/api-reference/?utm_source=github&utm_medium=site&utm_campaign=tolokakit#overview--accessing-the-api) to access Toloka API.

## Get Started
1. Install the Toloka-Kit package. Run the following command in the command shell:
```
$ pip install toloka-kit
```
For production environments, specify the exact package version. For the latest stable version, check the [project page at pypi.org](https://pypi.org/project/toloka-kit/).

**Note**: Starting with v1.0.0 release only the core version of the package is installed by default. See the Optional dependencies section for the details.

If you are just starting to use Toloka-Kit, the core package is enough. Our docs explicitly state which features require other packages, so you can install them later if you need them.

2. Check access to the API with the following Python script. The script imports the package, asks to enter the OAuth token, and requests general information about your account.
```python
import toloka.client as toloka
from getpass import getpass


# Uncomment one of the following two lines to specify where to send requests to: sandbox or production version of Toloka
target = 'SANDBOX'
# target = 'PRODUCTION'

toloka_client = toloka.TolokaClient(getpass("Enter your token:"), target)
print(toloka_client.get_requester())
```
If the code above has not raised any errors or exceptions, it means that everything works correctly.

3. Follow our [Learn the basics](https://github.com/Toloka/toloka-kit/blob/main/examples/0.getting_started/0.learn_the_basics/learn_the_basics.ipynb) tutorial to learn how to work with Toloka API using Toloka-Kit.

## Optional dependencies
Run this command to install toloka-kit with all additional dependencies:
```shell
$ pip install toloka-kit[all]
```
To install specific dependencies, run:
```shell
$ pip install toloka-kit[pandas,autoquality,s3,zookeeper,jupyter-metrics] # remove unnecessary requirements from the list
```

## Usage examples
[Toloka-kit usage examples](https://github.com/Toloka/toloka-kit/tree/main/examples#toloka-kit-usage-examples) - tutorials for specific data labeling tasks. They demonstrate how to work with Toloka API using Toloka-Kit.

## Documentation
* [Toloka-Kit documentation](https://toloka.ai/docs/toloka-kit/?utm_source=github&utm_medium=site&utm_campaign=tolokakit)
* [Toloka API reference](https://toloka.ai/docs/api/api-reference/?utm_source=github&utm_medium=site&utm_campaign=tolokakit)
* [Toloka web interface documentation](https://toloka.ai/docs/guide/overview/?utm_source=github&utm_medium=site&utm_campaign=tolokakit)

## Support
* To suggest a feature or report a bug, go to our [issues page](https://github.com/Toloka/toloka-kit/issues).
* If you have any questions, feel free to ask our [Slack community](https://toloka.ai/community/?utm_source=github&utm_medium=site&utm_campaign=tolokakit).

## Contributing
Feel free to contribute to toloka-kit. Right now, we need more [usage examples](https://github.com/Toloka/toloka-kit/tree/main/examples#need-more-examples).

## License
© Copyright 2023 Toloka team authors.
Licensed under the terms of the Apache License, Version 2.0. See [LICENSE](https://github.com/Toloka/toloka-kit/blob/main/LICENSE) for more details.
