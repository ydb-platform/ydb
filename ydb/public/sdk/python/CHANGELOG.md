## 2.2.0 ##

* allow to refer endpoints by node id
* support null type in queries

## 2.1.0 ##

* add compression support to ydb sdk

## 1.1.16 ##

* alias `kikimr.public.sdk.python.client` is deprecated. use `import ydb` instead.
* alias `kikimr.public.api` is deprecated, use `ydb.public.api` instead.
* method `construct_credentials_from_environ` is now deprecated and will be removed in the future.

## 1.1.15 ##

* override the default load balancing policy for discovery endpoint to the `round_robin` policy.

## 1.1.14 ##

* support session graceful shutdown protocol.

## 1.1.11 ##

* speedup driver initialization and first driver.wait

## 1.1.10 ##

 * add methods to request `_apis.TableService.RenameTables` call
 * by now the call is disabled at the server side
 * it will be enabled soon

## 1.1.0 ##

* remove useless `from_bytes` for PY3
* improve performance of column parser

## 1.0.27 ##

* fix bug with prepare in aio

## 1.0.26 ##

* allow specifying column labels in group by in the sqlalchemy dialect

## 1.0.25 ##

* add SEQUENCE to known schema types

## 1.0.22 ##

* add `retry_operation` to `aio.SessionPool`

## 1.0.21 ##

* ydb.aio supports retry operation helper.

## 1.0.20 ##

* storage class support

## 1.0.19 ##

* add async `SessionPool` support

## 1.0.18 ##

* minor change in dbapi

## 1.0.17 ##

* add asyncio support

## 1.0.14 ##

* add some custom datatypes for sqlalchemy

## 1.0.13 ##

* change error format for sqlalchemy

## 1.0.12 ##

* add ``ValueSinceUnixEpochModeSettings`` support to ``TtlSettings``

## 1.0.11 ##

* add default credentials constructor

## 1.0.10 ##

* pass issues to dbapi errors

## 1.0.9 ##

* allow custom user agent

## 1.0.8 ##

* load YDB certificates by default

## 1.0.7 ##

* allow to fail fast on driver wait

## 1.0.6 ##

* disable client query cache by default

## 1.0.3 ##

* support uuid type

## 1.0.1 ##

* Support ``store_size`` in describe of table.

## 1.0.0 ##

* Start initial changelog.
