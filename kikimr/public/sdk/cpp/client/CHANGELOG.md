## 2.2.0 ##

* Disable client query cache by default

## 2.1.0 ##

* Allow c++ sdk to create sessions in different ydb servers.

## 2.0.0 ##

* Remove request migrator feature from c++ sdk.

## 1.6.2 ##

* Fix SSL settings override for client

## 1.6.1 ##

* Do not wrap status in to CLIENT_DISCOVERY_FAILED in case of discovery error

## 1.6.0 ##

* Experimental interface to execute query from credential provider

## 1.5.1 ##

* Do not miss exceptions thrown by lambdas given to RetryOperation() method.

## 1.5.0 ##

* Added support of session graceful shutdown protocol

## 1.4.0 ##

* Support for socket timeout settings

## 1.3.0 ##

* Added user account key support for iam authentication

## 1.2.0 ##

* Added creation from environment support for DriverConfig

## 1.1.0 ##

* Support for grpc channel buffer limit settings

## 1.0.2 ##

* Added connection string support for CommonClientSettings, DriverConfig

## 1.0.1 ##

* Try to bind endpoint to session for
  BeginTransaction, CommitTransaction, RollbackTransaction, KeepAlive, CloseSession, ExplainDataQuery requests

## 1.0.0 ##

* Start initial changelog.
