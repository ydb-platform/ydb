[![PyPI Version][pypi-image]][pypi-url]
[![Build Status][build-image]][build-url]
[![License][license-image]][license-url]

<!-- Badges -->

[pypi-image]: https://img.shields.io/pypi/v/yandexcloud
[pypi-url]: https://pypi.org/project/yandexcloud/
[build-image]: https://github.com/yandex-cloud/python-sdk/actions/workflows/run-tests.yml/badge.svg
[build-url]: https://github.com/yandex-cloud/python-sdk/actions/workflows/run-tests.yml
[license-image]: https://img.shields.io/github/license/yandex-cloud/python-sdk.svg
[license-url]: https://github.com/yandex-cloud/python-sdk/blob/master/LICENSE

# Yandex.Cloud SDK (Python) 

Need to automate your infrastructure or use services provided by Yandex.Cloud? We've got you covered.

Installation:

    pip install yandexcloud

## Getting started

There are several options for authorization your requests - OAuth Token,
Metadata Service (if you're executing your code inside VMs or Cloud Functions
running in Yandex.Cloud), Service Account Keys, and externally created IAM tokens.

### OAuth Token

```python
sdk = yandexcloud.SDK(token='AQAD-.....')
```

### Metadata Service

Don't forget to assign Service Account for your Instance or Function and grant required roles.

```python
sdk = yandexcloud.SDK()
```

### Service Account Keys

```python
# you can store and read it from JSON file 
sa_key = {
    "id": "...",
    "service_account_id": "...",
    "private_key": "..."
}

sdk = yandexcloud.SDK(service_account_key=sa_key)
```

### IAM tokens

```python
sdk = yandexcloud.SDK(iam_token="t1.9eu...")
```

Check `examples` directory for more examples.

### Override service endpoint

#### Supported services

| Service Name                                                           | Alias                    |
|------------------------------------------------------------------------|--------------------------|
| yandex.cloud.ai.foundation_models                                      | ai-foundation-models     |
| yandex.cloud.ai.llm                                                    | ai-llm                   |
| yandex.cloud.ai.stt                                                    | ai-stt                   |
| yandex.cloud.ai.translate                                              | ai-translate             |
| yandex.cloud.ai.tts                                                    | ai-speechkit             |
| yandex.cloud.ai.vision                                                 | ai-vision                |
| yandex.cloud.apploadbalancer                                           | alb                      |
| yandex.cloud.billing                                                   | billing                  |
| yandex.cloud.cdn                                                       | cdn                      |
| yandex.cloud.certificatemanager.v1.certificate_content_service         | certificate-manager-data |
| yandex.cloud.certificatemanager                                        | certificate-manager      |
| yandex.cloud.compute                                                   | compute                  |
| yandex.cloud.containerregistry                                         | container-registry       |
| yandex.cloud.dataproc.manager                                          | dataproc-manager         |
| yandex.cloud.dataproc                                                  | dataproc                 |
| yandex.cloud.datatransfer                                              | datatransfer             |
| yandex.cloud.dns                                                       | dns                      |
| yandex.cloud.endpoint                                                  | endpoint                 |
| yandex.cloud.iam                                                       | iam                      |
| yandex.cloud.iot.devices                                               | iot-devices              |
| yandex.cloud.k8s                                                       | managed-kubernetes       |
| yandex.cloud.kms                                                       | kms                      |
| yandex.cloud.kms.v1.symmetric_crypto_service                           | kms-crypto               |
| yandex.cloud.loadbalancer                                              | load-balancer            |
| yandex.cloud.loadtesting                                               | loadtesting              |
| yandex.cloud.lockbox.v1.payload_service                                | lockbox-payload          |
| yandex.cloud.lockbox                                                   | lockbox                  |
| yandex.cloud.logging.v1.log_ingestion_service                          | log-ingestion            |
| yandex.cloud.logging.v1.log_reading_service                            | log-reading              |
| yandex.cloud.logging                                                   | logging                  |
| yandex.cloud.marketplace                                               | marketplace              |
| yandex.cloud.mdb.clickhouse                                            | managed-clickhouse       |
| yandex.cloud.mdb.elasticsearch                                         | managed-elasticsearch    |
| yandex.cloud.mdb.greenplum                                             | managed-greenplum        |
| yandex.cloud.mdb.kafka                                                 | managed-kafka            |
| yandex.cloud.mdb.mongodb                                               | managed-mongodb          |
| yandex.cloud.mdb.mysql                                                 | managed-mysql            |
| yandex.cloud.mdb.opensearch                                            | managed-opensearch       |
| yandex.cloud.mdb.postgresql                                            | managed-postgresql       |
| yandex.cloud.mdb.redis                                                 | managed-redis            |
| yandex.cloud.mdb.sqlserver                                             | managed-sqlserver        |
| yandex.cloud.operation                                                 | operation                |
| yandex.cloud.organizationmanager                                       | organization-manager     |
| yandex.cloud.resourcemanager                                           | resource-manager         |
| yandex.cloud.serverless.apigateway                                     | serverless-apigateway    |
| yandex.cloud.serverless.apigateway.websocket                           | apigateway-connections   |
| yandex.cloud.serverless.containers                                     | serverless-containers    |
| yandex.cloud.serverless.functions                                      | serverless-functions     |
| yandex.cloud.serverless.triggers                                       | serverless-triggers      |
| yandex.cloud.storage                                                   | storage-api              |
| yandex.cloud.vpc                                                       | vpc                      |
| yandex.cloud.ydb                                                       | ydb                      |


#### Override in client
```python
from yandex.cloud.vpc.v1.network_service_pb2_grpc import NetworkServiceStub
from yandexcloud import SDK

sdk = SDK(iam_token="t1.9eu...")
new_network_client_endpoint = "example.new.vpc.very.new.yandex:50051"
insecure = False # by default is False, but if server does not support verification can be set to True
network_client = sdk.client(NetworkServiceStub, endpoint=new_network_client_endpoint, insecure=False)
```

#### Override in sdk config
To override endpoints provide dict in format {alias : new-endpoint}
```python
from yandex.cloud.vpc.v1.network_service_pb2_grpc import NetworkServiceStub
from yandexcloud import SDK
new_network_client_endpoint = "example.new.vpc.very.new.yandex:50051"
sdk = SDK(iam_token="t1.9eu...", endpoints={"vpc": new_network_client_endpoint})
insecure = False # by default is False, but if server does not support verification can be set to True
network_client = sdk.client(NetworkServiceStub, insecure=False)
```

Notice: if both overrides are used for same endpoint, override by client has priority

#### Switch SDK region
```python
from yandexcloud import SDK, set_up_yc_api_endpoint
kz_region_endpoint = "api.yandexcloud.kz"
# this will make SDK list endpoints from KZ yc installation 
sdk = SDK(iam_token="t1.9eu...", endpoint="api.yandexcloud.kz")
# or you can use global function
set_up_yc_api_endpoint(kz_region_endpoint)
```

## Contributing
### Dependencies
Use `make deps` command to install library, its production and development dependencies.

### Formatting
Use `make format` to autoformat code with black tool. 

### Tests
- `make test` to run tests for current python version
- `make lint` to run only linters for current python version
- `make tox-current` to run all checks (tests + code style checks + linters + format check) for current python version
- `make tox` to run all checks for all supported (installed in your system) python versions
- `make test-all-versions` to run all checks for all supported python versions in docker container


### Maintaining
If pull request consists of several meaningful commits, that should be preserved, 
then use "Rebase and merge" option. Otherwise use "Squash and merge". 

New release (changelog, tag and pypi upload) will be automatically created 
on each push to master via Github Actions workflow.
