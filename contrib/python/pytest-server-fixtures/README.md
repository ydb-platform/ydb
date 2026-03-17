# Pytest Server Fixtures

This library provides an extensible framework for running up real network
servers in your tests, as well as a suite of fixtures for some well-known webservices
and databases.

## Table of Contents
* [Batteries Included](#batteries-included)
* [Installation](#installation)
* [Configuration](#configuration)
* [Common fixture properties](#common-fixture-properties)
* [MongoDB](#mongodb)
* [Postgres](#postgres)
* [Redis](#redis)
* [S3 Minio](#s3-minio)
* [Apache httpd](#apache-httpd)
* [Simple HTTP Server](#simple-http-server)
* [Xvfb](#xvfp)
* [Jenkins](#jenkins)
* [Server Framework](#server-framework)
* [Integration Tests](#integration-tests)


## Batteries Included


| Fixture | Extra Dependency Name
| - | -
| MongoDB | mongodb
| Postgres | postgres
| Redis | redis
| S3 Minio | s3
| Apache Httpd | <none>
| Simple HTTP Server | <none>
| Jenkins | jenkins
| Xvfb (X-Windows Virtual Frame Buffer) | <none>

Note: v2 fixtures support launching fixtures locally, in `Docker` containers
or as `Kubernetes` pods (See [Configuration](#configuration))


## Installation

Installation of this package varies on which parts of it you would like to use.
It uses optional dependencies (specified in the table above) to reduce the number
of 3rd party packages required. This way if you don't use MongoDB, you don't need
to install PyMongo.

```bash
    # Install with support for just mongodb
    pip install pytest-server-fixtures[mongodb]

    # Install with support for mongodb and jenkins
    pip install pytest-server-fixtures[mongodb,jenkins]

    # Install with Docker support
    pip install pytest-server-fixtures[docker]

    # Install with Kubernetes support
    pip install pytest-server-fixtures[kubernetes]

    # Install with only core library and support for httpd and xvfp
    pip install pytest-server-fixtures
```

Enable the fixture explicitly in your tests or conftest.py (not required when using setuptools
entry points):

```python
    pytest_plugins = ['pytest_server_fixtures.httpd',
                      'pytest_server_fixtures.jenkins',
                      'pytest_server_fixtures.mongo',
                      'pytest_server_fixtures.postgres',
                      'pytest_server_fixtures.redis',
                      'pytest_server_fixtures.xvfb',
                      ]
```

## Configuration

The fixtures are configured using the following evironment variables:

| Setting | Description | Default
| ------- | ----------- | -------
| `SERVER_FIXTURES_HOSTNAME`      | Hostname that servers will listen on | Current default hostname
| `SERVER_FIXTURES_DISABLE_HTTP_PROXY` | Disable any HTTP proxies set up in the shell environment when making HTTP requests | True
| `SERVER_FIXTURES_SERVER_CLASS` | Server class used to run the fixtures, choose from `thread`, `docker` and `kubernetes` | `thread`
| `SERVER_FIXTURES_K8S_NAMESPACE` | (Kubernetes only) Specify the Kubernetes namespace used to launch fixtures. | `None` (same as the test host)
| `SERVER_FIXTURES_K8S_LOCAL_TEST` | (Kubernetes only) Set to `True` to allow integration tests to run (See [Integration Tests](#integration-tests)). | `False`
| `SERVER_FIXTURES_MONGO_BIN`     | Absolute path to mongod executable | "" (relies on mongod access via `$PATH`)
| `SERVER_FIXTURES_MONGO_IMAGE`   | (Docker only) Docker image for mongo | `mongo:3.6`
| `SERVER_FIXTURES_PG_CONFIG`     | Postgres pg_config executable | `pg_config`
| `SERVER_FIXTURES_REDIS`         | Redis server executable | `redis-server`
| `SERVER_FIXTURES_REDIS_IMAGE`   | (Docker only) Docker image for redis | `redis:5.0.2-alpine`
| `SERVER_FIXTURES_HTTPD`         | Httpd server executable | `apache2`
| `SERVER_FIXTURES_HTTPD_MODULES` | Httpd modules directory | `/usr/lib/apache2/modules`
| `SERVER_FIXTURES_JAVA`          | Java executable used for running Jenkins server | `java`
| `SERVER_FIXTURES_JENKINS_WAR`   | `.war` file used to run Jenkins | `/usr/share/jenkins/jenkins.war`
| `SERVER_FIXTURES_XVFB`          | Xvfb server executable | `Xvfb`

## Common fixture properties

All of these fixtures follow the pattern of spinning up a server on a unique port and
then killing the server and cleaning up on fixture teardown.

All test fixtures share the following properties at runtime:

| Property | Description
| -------- | -----------
| `hostname`  | Hostname that server is listening on
| `port`      | Port number that the server is listening on
| `dead`      | True/False: am I dead yet?
| `workspace` | [`path`](https://path.readthedocs.io/) object for the temporary directory the server is running out of

## MongoDB

The `mongo` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `mongo_server`      | Function-scoped MongoDB server
| `mongo_server_sess` | Session-scoped MongoDB server
| `mongo_server_cls`  | Class-scoped MongoDB server

All these fixtures have the following properties:

| Property | Description
| -------- | -----------
| `api` | `pymongo.MongoClient` connected to running server

Here's an example on how to run up one of these servers:

```python
def test_mongo(mongo_server):
    db = mongo_server.api.mydb
    collection = db.test_coll
    test_coll.insert({'foo': 'bar'})
    assert test_coll.find_one()['foo'] == 'bar'
```

## Postgres
The `postgres` module contains the following fixture:

| Fixture Name | Description
| ------------ | -----------
| `postgres_server_sess` | Session-scoped Postgres server

The Postgres server fixture has the following properties:

| Property | Description
| -------- | -----------
| `connect()` | Returns a raw `psycopg2` connection object connected to the server
| `connection_config` | Returns a dict containing all the data needed for another db library to connect with.

You may wish to build another fixture on top of the session-scoped fixture; for example:
```python
def create_full_schema(connection):
    """Create the database schema"""
    pass

@pytest.fixture(scope='session')
def db_config_sess(postgres_server_sess: PostgresServer) -> PostgresServer:
    """Returns a DbConfig pointing at a fully-created db schema"""
    server_cfg = postgres_server_sess.connection_config
    create_full_schema(postgres_server_sess.connect())
    return postgres_server_sess
```

## Redis

The `redis` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `redis_server`      | Function-scoped Redis server
| `redis_server_sess` | Session-scoped Redis server

All these fixtures have the following properties:

| Property | Description
| -------- | -----------
| `api` | `redis.Redis` client connected to the running server

Here's an example on how to run up one of these servers:

```python
def test_redis(redis_server):
    redis_server.api.set('foo': 'bar')
    assert redis_server.api.get('foo') == 'bar'
```

## S3 Minio

The `s3` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `s3_server`  | Session-scoped S3 server using the 'minio' tool.
| `s3_bucket`  | Function-scoped S3 bucket


The S3 server has the following properties:

| Property | Description
| -------- | -----------
| `get_s3_client()` | Return a boto3 `Resource`: (`boto3.resource('s3', ...)`

The S3 Bucket has the following properties:

| Property | Description
| -------- | -----------
| `name`   | Bucket name, a UUID
| `client` | Boto3 `Resource` from the server


Here's an example on how to run up one of these servers:

```python
def test_connection(s3_bucket):
    bucket = s3_bucket.client.Bucket(s3_bucket.name)
    assert bucket is not None
```

# Apache httpd

The `httpd` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `httpd_server` | Function-scoped httpd server to use as a web proxy

The fixture has the following properties at runtime:

| Property | Description
| -------- | -----------
| `document_root` | `path.path` to the document root
| `log_dir` | `path.path` to the log directory

Here's an example showing some of the features of the fixture:

```python
def test_httpd(httpd_server):
    # Log files can be accessed by the log_dir property
    assert 'access.log' in [i.basename() for i in httpd_server.log_dir.files()]

    # Files in the document_root are accessable by HTTP
    hello = httpd_server.document_root / 'hello.txt'
    hello.write_text('Hello World!')
    response = httpd_server.get('/hello.txt')
    assert response.status_code == 200
    assert response.text == 'Hello World!'
```

## Proxy Rules

An httpd server on its own isn't super-useful, so the underlying class for the
fixture has options for configuring it as a reverse proxy. Here's an example
where we've pulled in a `pytest-pyramid` fixture and set it up to be proxied
from the `httpd` server:

```python
import pytest
from pytest_server_fixtures.httpd import HTTPDServer

pytest_plugins=['pytest_pyramid']

@pytest.yield_fixture()
def proxy_server(pyramid_server):

    # Configure the proxy rules as a dict of source -> dest URLs
    proxy_rules = {'/downstream/' : pyramid_server.url
                  }

    server = HTTPDServer(proxy_rules,
                         # You can also specify any arbitrary text you want to
                         # put in the config file
                         extra_cfg = 'Alias /tmp /var/tmp\n',
                         )
    server.start()
    yield server
    server.teardown()

def test_proxy(proxy_server):
    # This request will be proxied to the pyramid server
    response = proxy_server.get('/downstream/accounts')
    assert response.status_code == 200
```

# Simple HTTP Server

The `http` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `simple_http_server` | Function-scoped instance of Python's `SimpleHTTPServer`

The fixture has the following properties at runtime:

| Property | Description
| -------- | -----------
| `document_root` | `path.path` to the document root

Here's an example showing some of the features of the fixture:

```python
def test_simple_server(simple_http_server):
    # Files in the document_root are accessable by HTTP
    hello = simple_http_server.document_root / 'hello.txt'
    hello.write_text('Hello World!')
    response = simple_http_server.get('/hello.txt')
    assert response.status_code == 200
    assert response.text == 'Hello World!'
```

# Jenkins

The `jenkins` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `jenkins_server` | Session-scoped Jenkins server instance

The fixture has the following methods and properties:

| Property | Description
| -------- | -----------
| `api` | `jenkins.Jenkins` API client connected to the running server (see https://python-jenkins.readthedocs.org)
| `load_plugins()` | Load plugins into the server from a directory


Here's an example showing how to run up the server:

```python
PLUGIN_DIR='/path/to/some/plugins'

def test_jenkins(jenkins_server):
    jenkins_server.load_plugins(PLUGIN_DIR)
    assert not jenkins_server.api.get_jobs()
```

# Xvfb

The `xvfb` module contains the following fixtures:

| Fixture Name | Description
| ------------ | -----------
| `xvfb_server` | Function-scoped Xvfb server
| `xvfb_server_sess` | Session-scoped Xvfb server

The fixture has the following properties:

| Property | Description
| -------- | -----------
| `display` | X-windows `DISPLAY` variable

Here's an example showing how to run up the server:

```python
def test_xvfb(xvfb_server):
    assert xvfb_server.display
```


# Server Framework

All the included fixtures and others in this suite of plugins are built on an extensible
TCP server running framework, and as such many of them share various properties and methods.

```
pytest_shutil.workspace.Workspace
  |
  *--base2.TestServerV2
     |
     *--mongo.MongoTestServer
     *--redis.RedisTestServer
  *--base.TestServer
     |
     *--http.HTTPTestServer
        |
        *--http.SimpleHTTPTestServer
        *--httpd.HTTPDServer
        *--jenkins.JenkinsTestServer
        *--pytest_pyramid.PyramidTestServer
```

## Class Methods

The best way to understand the framework is look at the code, but here's a quick summary
on the class methods that child classes of `base.TestServer` can override.

| Method | Description
| ------ | -----------
| `pre_setup`                  | This should execute any setup required before starting the server
| `run_cmd` (required)         | This should return a list of shell commands needed to start the server
| `run_stdin`                  | The result of this is passed to the process as stdin
| `check_server_up` (required) | This is called to see if the server is running
| `post_setup`                 | This should execute any setup required after starting the server

## Class Attributes

At a minimum child classes must define `run_cmd` and `check_server_up`.
There are also some class attributes that can be overridden to modify server behavior:

| Attribute | Description | Default
| --------- | ----------- | -------
| `random_port`      | Start the server on a guaranteed unique random TCP port  | True
| `port_seed`        | If `random_port` is false, port number is semi-repeatable and based on a hash of the class name and this seed. | 65535
| `kill_signal`      | Signal used to kill the server | `SIGTERM`
| `kill_retry_delay` | Number of seconds to wait between kill retries. Increase this if your server takes a while to die | 1

## Constructor Arguments

The base class constructor also accepts these arguments:

| Argument | Description
| -------- | -----------
| `port`                  | Explicitly set the port number
| `hostname` | Explicitly set the hostname
| `env` | Dict of the shell environment passed to the server process
| `cwd` | Override the current working directory of the server process

# Integration Tests

```
$ vagrant up
$ vagrant ssh
...
$ . venv/bin/activate
$ cd /vagrant
$ make develop
$ cd pytest-server-fixtures

# test serverclass="thread"
$ pytest

# test serverclass="docker"
$ SERVER_FIXTURES_SERVER_CLASS=docker pytest

# test serverclass="kubernetes"
$ SERVER_FIXTURES_SERVER_CLASS=kubernetes SERVER_FIXTURES_K8S_LOCAL_TEST=True pytest
```
