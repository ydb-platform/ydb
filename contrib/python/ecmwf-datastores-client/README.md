<p align="center">
  <a href="https://github.com/ecmwf/codex/raw/refs/heads/main/ESEE">
    <img src="https://github.com/ecmwf/codex/raw/refs/heads/main/ESEE/data_provision_badge.svg" alt="ECMWF Software EnginE"></a>
  <a href="https://github.com/ecmwf/codex/raw/refs/heads/main/Project Maturity">
    <img src="https://github.com/ecmwf/codex/raw/refs/heads/main/Project Maturity/incubating_badge.svg" alt="Maturity Level"></a>
  <!-- <a href="https://codecov.io/gh/ecmwf/ecmwf-datastores-client">
    <img src="https://codecov.io/gh/ecmwf/ecmwf-datastores-client/branch/main/graph/badge.svg" alt="Code Coverage"></a> -->
  <a href="https://opensource.org/licenses/apache-2-0">
    <img src="https://img.shields.io/badge/Licence-Apache 2.0-blue.svg" alt="Licence"></a>
  <a href="https://github.com/ecmwf/ecmwf-datastores-client/releases">
    <img src="https://img.shields.io/github/v/release/ecmwf/ecmwf-datastores-client?color=purple&label=Release" alt="Latest Release"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a>
  •
  <a href="#installation">Installation</a>
  •
  <a href="https://ecmwf.github.io/ecmwf-datastores-client/">Documentation</a>
</p>

> [!IMPORTANT]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

# ecmwf-datastores-client

ECMWF Data Stores Service (DSS) API Python client.

Technical documentation: https://ecmwf.github.io/ecmwf-datastores-client/

## Installation

Install with conda:

```
$ conda install -c conda-forge ecmwf-datastores-client
```

Install with pip:

```
$ pip install ecmwf-datastores-client
```

## Configuration

The `Client` requires the `url` to the API root and a valid API `key`. These can be provided in three ways, in order of precedence:

1. As keyword arguments when instantiating the `Client`.
1. Via the `ECMWF_DATASTORES_URL` and `ECMWF_DATASTORES_KEY` environment variables.
1. From a configuration file, which must be located at `~/.ecmwfdatastoresrc` or at the path specified by the `ECMWF_DATASTORES_RC_FILE` environment variable.

```
$ cat $HOME/.ecmwfdatastoresrc
url: https://cds.climate.copernicus.eu/api
key: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

## Quick Start

Configure the logging level to display INFO messages:

```python
>>> import logging
>>> logging.basicConfig(level="INFO")

```

Instantiate the API client and optionally verify authentication:

```python
>>> from ecmwf.datastores import Client
>>> client = Client()
>>> client.check_authentication()  # optional check
{...}

```

Retrieve data:

```python
>>> collection_id = "reanalysis-era5-pressure-levels"
>>> request = {
...     "product_type": ["reanalysis"],
...     "variable": ["temperature"],
...     "year": ["2022"],
...     "month": ["01"],
...     "day": ["01"],
...     "time": ["00:00"],
...     "pressure_level": ["1000"],
...     "data_format": "grib",
...     "download_format": "unarchived",
... }

>>> client.retrieve(collection_id, request, target="target_1.grib")  # blocks
'target_1.grib'

```

Alternative methods to retrieve data:

```python
>>> remote = client.submit(collection_id, request)  # doesn't block
>>> remote
Remote(...)
>>> remote.download("target_2.grib")  # blocks
'target_2.grib'

>>> results = client.submit_and_wait_on_results(collection_id, request)  # blocks
>>> results
Results(...)
>>> results.download("target_3.grib")
'target_3.grib'

>>> client.download_results(remote.request_id, "target_4.grib")  # blocks
'target_4.grib'

```

List all collection IDs sorted by last update:

```python
>>> collections = client.get_collections(sortby="update")

>>> collection_ids = []
>>> while collections is not None:  # Loop over pages
...     collection_ids.extend(collections.collection_ids)
...     collections = collections.next  # Move to the next page
...

>>> collection_ids
[...]
>>> collection_id in collection_ids
True

```

Explore a collection:

```python
>>> collection = client.get_collection(collection_id)

>>> collection.id == collection_id
True
>>> collection.title
'...'
>>> collection.description
'...'

>>> collection.published_at
datetime.datetime(...)
>>> collection.updated_at
datetime.datetime(...)

>>> collection.begin_datetime
datetime.datetime(...)
>>> collection.end_datetime
datetime.datetime(...)
>>> collection.bbox
(...)

>>> collection.submit(request)
Remote(...)

>>> collection.apply_constraints(request)
{...}

```

Interact with results:

```python
>>> results = client.get_results(remote.request_id)

>>> results.content_length > 0
True
>>> results.content_type
'application/x-grib'
>>> results.location
'...'

>>> results.download("target_5.grib")
'target_5.grib'

```

List all successful jobs, sorted by newest first:

```python
>>> jobs = client.get_jobs(sortby="-created", status="successful")

>>> request_ids = []
>>> while jobs is not None:  # Loop over pages
...     request_ids.extend(jobs.request_ids)
...     jobs = jobs.next  # Move to the next page
...

>>> request_ids
[...]
>>> remote.request_id in request_ids
True

```

Interact with a previously submitted job:

```python
>>> remote = client.get_remote(remote.request_id)

>>> remote.collection_id == collection_id
True
>>> remote.request
{...}

>>> remote.status
'successful'
>>> remote.results_ready
True

>>> remote.created_at
datetime.datetime(...)
>>> remote.started_at
datetime.datetime(...)
>>> remote.finished_at
datetime.datetime(...)
>>> remote.updated_at == remote.finished_at
True

>>> remote.download("target_6.grib")
'target_6.grib'

>>> remote.get_results()
Results(...)

>>> remote.delete()
{...}

```

Apply constraints and find the number of available days in a given month:

```python
>>> month = {"year": "2000", "month": "02"}
>>> constrained_request = client.apply_constraints(collection_id, month)

>>> len(constrained_request["day"])
29

```

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.12:

```
conda create -n DEVELOP -c conda-forge python=3.12
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
