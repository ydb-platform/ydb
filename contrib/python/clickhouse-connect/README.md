## ClickHouse Connect

A high performance core database driver for connecting ClickHouse to Python, Pandas, and Superset
* Pandas DataFrames
* Numpy Arrays
* PyArrow Tables
* Superset Connector
* SQLAlchemy 1.3 and 1.4 (limited feature set)

ClickHouse Connect currently uses the ClickHouse HTTP interface for maximum compatibility.  


### Installation

```
pip install clickhouse-connect
```

ClickHouse Connect requires Python 3.8 or higher. 


### Superset Connectivity
ClickHouse Connect is fully integrated with Apache Superset.  Previous versions of ClickHouse Connect utilized a
dynamically loaded Superset Engine Spec, but as of Superset v2.1.0 the engine spec was incorporated into the main
Apache Superset project and removed from clickhouse-connect in v0.6.0.  If you have issues connecting to earlier
versions of Superset, please use clickhouse-connect v0.5.25.

When creating a Superset Data Source, either use the provided connection dialog, or a SqlAlchemy DSN in the form
`clickhousedb://{username}:{password}@{host}:{port}`.


### SQLAlchemy Implementation
ClickHouse Connect incorporates a minimal SQLAlchemy implementation (without any ORM features) for compatibility with
Superset.  It has only been tested against SQLAlchemy versions 1.3.x and 1.4.x, and is unlikely to work with more
complex SQLAlchemy applications.


### Complete Documentation
The documentation for ClickHouse Connect has moved to
[ClickHouse Docs](https://clickhouse.com/docs/en/integrations/language-clients/python/intro) 


 
