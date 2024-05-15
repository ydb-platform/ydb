## Hive Metastore Client
_A client for connecting and running DDLs on [Hive](https://hive.apache.org/) Metastore using [Thrift](https://thrift.apache.org/) protocol._

<img height="200" src="logo_hive_metastore_client.jpg" />

[![Release](https://img.shields.io/github/v/release/quintoandar/hive-metastore-client)]((https://pypi.org/project/hive-metastore-client/))
![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8-brightgreen.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

| Source    | Downloads                                                                                                                       | Page                                                 | Installation Command                       |
|-----------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|--------------------------------------------|
| **PyPi**  | [![PyPi Downloads](https://pepy.tech/badge/hive-metastore-client)](https://pypi.org/project/hive-metastore-client/)                      | [Link](https://pypi.org/project/hive-metastore-client/)        | `pip install hive-metastore-client `                  |

### Build status
| Develop                                                                     | Stable                                                                            | Documentation                                                                                                                                           | Sonar                                                                                                                                                                                    |
|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ![Test](https://github.com/quintoandar/hive-metastore-client/workflows/Test/badge.svg) | ![Publish](https://github.com/quintoandar/hive-metastore-client/workflows/Publish/badge.svg) | [![Documentation Status](https://readthedocs.org/projects/hive-metastore-client/badge/?version=latest)](https://hive-metastore-client.readthedocs.io/en/latest/?badge=latest) | [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=quintoandar_hive-metastore-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=quintoandar_hive-metastore-client) |

This library supports Python version 3.7+.

To check library main features you can check [Hive Metastore Client's Documentation](https://hive-metastore-client.readthedocs.io/en/latest/), which is hosted by Read the Docs.

An example of how to use the library for running DDL commands in hive metastore:

```python
from hive_metastore_client.builders import DatabaseBuilder
from hive_metastore_client import HiveMetastoreClient

database = DatabaseBuilder(name='new_db').build()
with HiveMetastoreClient(HIVE_HOST, HIVE_PORT) as hive_metastore_client:
    hive_metastore_client.create_database(database) 
```

To learn more use cases in practice, see [Hive Metastore Client examples](https://github.com/quintoandar/hive-metastore-client/tree/main/examples)  

## Requirements and Installation
Hive Metastore Client depends on **Python 3.7+**

[Python Package Index](https://pypi.org/project/hive-metastore-client/) hosts reference to a pip-installable module of this library, using it is as straightforward as including it on your project's requirements.

```bash
pip install hive-metastore-client
```

## License
[Apache License 2.0](https://github.com/quintoandar/hive-metastore-client/blob/main/LICENSE)

## Contributing
All contributions are welcome! Feel free to open Pull Requests. Check the development and contributing **guidelines** 
described in [CONTRIBUTING.md](https://github.com/quintoandar/hive-metastore-client/blob/main/CONTRIBUTING.md)

Made with :heart: by the **Data Engineering** team from [QuintoAndar](https://github.com/quintoandar/)
