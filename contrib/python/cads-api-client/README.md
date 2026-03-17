# cads-api-client

CADS API Python client

Technical documentation: https://ecmwf-projects.github.io/cads-api-client/

## Configuration

The `ApiClient` requires the `url` to the API root and a valid API `key`. You can also set the `CADS_API_URL` and `CADS_API_KEY` environment variables, or use a configuration file. The configuration file must be located at `~/.cads-api-client.json`, or at the path specified by the `CADS_API_RC` environment variable.

```
$ cat $HOME/.cads-api-client.json
{"url": "https://cds.climate.copernicus.eu/api", "key": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}
```

It is possible (though not recommended) to use the API key of one of the test users:

```
00112233-4455-6677-c899-aabbccddeeff
```

This key is used for anonymous tests and is designed to be the least performant option for accessing the system.

## Quick Start

Configure the logging level to display INFO messages:

```python
>>> import logging
>>> logging.basicConfig(level="INFO")

```

Instantiate the API client and verify the authentication:

```python
>>> import os
>>> from cads_api_client import ApiClient
>>> client = ApiClient(
...     url=os.getenv("CADS_API_URL"),
...     key=os.getenv("CADS_API_KEY"),
... )
>>> client.check_authentication()
{...}

```

Explore a collection:

```python
>>> collection_id = "reanalysis-era5-pressure-levels"
>>> collection = client.get_collection(collection_id)
>>> collection.end_datetime
datetime.datetime(...)

>>> request = {
...     "product_type": "reanalysis",
...     "variable": "temperature",
...     "year": "2022",
...     "month": "01",
...     "day": "01",
...     "pressure_level": "1000",
...     "time": "00:00",
... }
>>> collection.process.apply_constraints(**request)
{...}

```

Retrieve data:

```python
>>> client.retrieve(collection_id, target="tmp1-era5.grib", **request)  # blocks
'tmp1-era5.grib'

>>> remote = client.submit(collection_id, **request)  # doesn't block
>>> remote.request_uid
'...'
>>> remote.status
'...'
>>> remote.download("tmp2-era5.grib")  # blocks
'tmp2-era5.grib'

```

Interact with a previously submitted job:

```python
>>> remote = client.get_remote(remote.request_uid)
>>> remote.delete()
{...}

```

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.11:

```
conda create -n DEVELOP -c conda-forge python=3.11
conda activate DEVELOP
```

Before pushing to GitHub, run the following commands:

1. Update conda environment: `make conda-env-update`
1. Install this package: `pip install -e .`
1. Sync with the latest [template](https://github.com/ecmwf-projects/cookiecutter-conda-package) (optional): `make template-update`
1. Run quality assurance checks: `make qa`
1. Run tests: `make unit-tests`
1. Run the static type checker: `make type-check`
1. Build the documentation (see [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/)): `make docs-build`

## License

```
Copyright 2022, European Union.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
