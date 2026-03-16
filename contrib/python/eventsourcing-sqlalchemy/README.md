# Event Sourcing in Python with SQLAlchemy

This package supports using the Python
[eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library
with [SQLAlchemy](https://www.sqlalchemy.org/).

## Table of contents

<!-- TOC -->
* [Table of contents](#table-of-contents)
* [Quick start](#quick-start)
* [Installation](#installation)
* [Getting started](#getting-started)
* [Managing transactions outside the application](#managing-transactions-outside-the-application)
* [Using SQLAlchemy scoped sessions](#using-sqlalchemy-scoped-sessions)
* [Managing sessions with Flask-SQLAlchemy](#managing-sessions-with-flask-sqlalchemy)
* [Managing sessions with FastAPI-SQLAlchemy](#managing-sessions-with-fastapi-sqlalchemy)
* [Google Cloud SQL Python Connector](#google-cloud-sql-python-connector)
* [More information](#more-information)
<!-- TOC -->

## Quick start

To use SQLAlchemy with your Python eventsourcing applications:
* install the Python package `eventsourcing_sqlalchemy`
* set the environment variable `PERSISTENCE_MODULE` to `'eventsourcing_sqlalchemy'`
* set the environment variable `SQLALCHEMY_URL` to an SQLAlchemy database URL

See below for more information.

## Installation

Use pip to install the [stable distribution](https://pypi.org/project/eventsourcing_sqlalchemy/)
from the Python Package Index. Please note, it is recommended to
install Python packages into a Python virtual environment.

    $ pip install eventsourcing_sqlalchemy

## Getting started

Define aggregates and applications in the usual way.

```python
from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event
from uuid import uuid5, NAMESPACE_URL


class TrainingSchool(Application):
    def register(self, name):
        dog = Dog(name)
        self.save(dog)

    def add_trick(self, name, trick):
        dog = self.repository.get(Dog.create_id(name))
        dog.add_trick(trick)
        self.save(dog)

    def get_tricks(self, name):
        dog = self.repository.get(Dog.create_id(name))
        return dog.tricks


class Dog(Aggregate):
    @event('Registered')
    def __init__(self, name):
        self.name = name
        self.tricks = []

    @staticmethod
    def create_id(name):
        return uuid5(NAMESPACE_URL, f'/dogs/{name}')

    @event('TrickAdded')
    def add_trick(self, trick):
        self.tricks.append(trick)
```

To use this module as the persistence module for your application, set the environment
variable `PERSISTENCE_MODULE` to `'eventsourcing_sqlalchemy'`.

When using this module, you need to set the environment variable `SQLALCHEMY_URL` to an
SQLAlchemy database URL for your database.
Please refer to the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/14/core/engines.html)
for more information about SQLAlchemy Database URLs.

Optionally, you can also configure SQLAlchemy to use a database schema, by setting
the environment variable `SQLALCHEMY_SCHEMA`. This is not supported on all databases,
but has been tested with PostgreSQL.

The example below uses SQLAlchemy with an in-memory SQLite database.

```python
import os

os.environ['PERSISTENCE_MODULE'] = 'eventsourcing_sqlalchemy'
os.environ['SQLALCHEMY_URL'] = 'sqlite:///:memory:'
```

Construct and use the application in the usual way.

```python
school = TrainingSchool()
school.register('Fido')
school.add_trick('Fido', 'roll over')
school.add_trick('Fido', 'play dead')
tricks = school.get_tricks('Fido')
assert tricks == ['roll over', 'play dead']
```

## Managing transactions outside the application

Sometimes you might need to update an SQLAlchemy ORM model atomically with updates to
your event-sourced application. You can manage transactions outside the application.
Just call the application recorder's `transaction()` method and use the returned
`Transaction` object as a context manager to obtain an SQLAlchemy `Session`
object. You can `add()` your ORM objects to the session. Everything will commit
atomically when the `Transaction` context manager exits. This effectively implements
thread-scoped transactions.

```python
with school.recorder.transaction() as session:
    # Update CRUD model.
    ...  # session.add(my_orm_object)
    # Update event-sourced application.
    school.register('Buster')
    school.add_trick('Buster', 'fetch ball')

    tricks = school.get_tricks('Buster')
    assert tricks == ['fetch ball']
```

Please note, the SQLAlchemy "autoflush" ORM feature is enabled by default.

```python
app = Application()

with app.recorder.transaction() as session:
    assert session.autoflush is True
```

If you need "autoflush" to be disabled, you can set the environment variable `SQLALCHEMY_NO_AUTOFLUSH`.

```python
app = Application(env={'SQLALCHEMY_AUTOFLUSH': 'False'})

with app.recorder.transaction() as session:
    assert session.autoflush is False
```

Alternatively, you can set the autoflush option directly on the SQLAlchemy session maker.

```python
app = Application()
app.recorder.datastore.session_maker.kw["autoflush"] = False

with app.recorder.transaction() as session:
    assert session.autoflush is False
```

Alternatively, you can use the session's ``no_autoflush`` context manager.

```python
app = Application()

with app.recorder.transaction() as session:
    with session.no_autoflush:
        assert session.autoflush is False
```

Alternatively, you can set the ``autoflush`` attribute of the session object.

```python
app = Application()

with app.recorder.transaction() as session:
    session.autoflush = False
    # Add CRUD objects to the session.
    ...
```

## Using SQLAlchemy scoped sessions

It's possible to configure the application to use an SQLAlchemy `scoped_session`
object which will scope the session to standard threads, or other things such
as Web requests in a Web application framework.

Define an adapter for a `scoped_session` object and configure the event-sourced
application using the environment variable `SQLALCHEMY_SCOPED_SESSION_TOPIC`.

```python
from eventsourcing.application import AggregateNotFoundError
from eventsourcing.utils import get_topic
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

# Create engine.
engine = create_engine('sqlite:///:memory:')

# Create a scoped_session object.
session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

# Define an adapter for the scoped session.
class MyScopedSessionAdapter:
    def __getattribute__(self, item: str) -> None:
        return getattr(session, item)

# Produce the topic of the scoped session adapter class.
scoped_session_topic = get_topic(MyScopedSessionAdapter)

# Construct an event-sourced application.
app = Application(
    env={'SQLALCHEMY_SCOPED_SESSION_TOPIC': scoped_session_topic}
)

# During request.
aggregate = Aggregate()
app.save(aggregate)
app.repository.get(aggregate.id)
session.commit()

# After request.
session.remove()

# During request.
app.repository.get(aggregate.id)

# After request.
session.remove()

# During request.
aggregate = Aggregate()
app.save(aggregate)
# forget to commit

# After request.
session.remove()

# During request.
try:
    # forgot to commit
    app.repository.get(aggregate.id)
except AggregateNotFoundError:
    pass
else:
    raise Exception("Expected aggregate not found")

# After request.
session.remove()
```

As you can see, you need to call `commit()` during a request, and call `remove()`
after the request completes. Packages that integrate SQLAlchemy with Web application
frameworks tend to automate this call to `remove()`. Some of them also call `commit()`
automatically if an exception is not raised during the handling of a request.

## Managing sessions with Flask-SQLAlchemy

The package [Flask-SQLAlchemy](https://github.com/pallets-eco/flask-sqlalchemy)
([full docs](https://flask-sqlalchemy.palletsprojects.com/)) provides a class
called `SQLAlchemy` which has a `session` attribute which is an SQLAlchemy
`scoped_session`. This can be adapted in a similar way.

```python
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
try:
    from sqlalchemy.orm import declarative_base  # type: ignore
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

# Define a Flask app.
flask_app = Flask(__name__)
flask_app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'

# Integration between Flask and SQLAlchemy.
Base = declarative_base()
db = SQLAlchemy(flask_app, model_class=Base)


# Define an adapter for the scoped session.
class FlaskScopedSession:
    def __getattribute__(self, item: str) -> None:
        return getattr(db.session, item)


# Run the Flask application in a Web application server.
with flask_app.app_context():

    # Produce the topic of the scoped session adapter class.
    scoped_session_adapter_topic = get_topic(FlaskScopedSession)
    # Construct event-sourced application to use scoped sessions.
    es_app = Application(
        env={"SQLALCHEMY_SCOPED_SESSION_TOPIC": scoped_session_adapter_topic}
    )

    # During request.
    aggregate = Aggregate()
    es_app.save(aggregate)
    db.session.commit()

    # After request (this is done automatically).
    db.session.remove()

    # During request.
    es_app.repository.get(aggregate.id)

    # After request (this is done automatically).
    db.session.remove()
```

## Managing sessions with FastAPI-SQLAlchemy

The package [FastAPI-SQLAlchemy](https://github.com/mfreeborn/fastapi-sqlalchemy)
doesn't actually use an SQLAlchemy `scoped_session`, but instead has a global `db`
variable that has a `session` attribute which returns request-scoped sessions when
accessed. This can be adapted in a similar way. Sessions are committed automatically
after the request has been handled successfully, and not committed if an exception
is raised.

```python
from fastapi import FastAPI
from fastapi_sqlalchemy import db, DBSessionMiddleware

# Define a FastAPI application.
fastapi_app = FastAPI()

# Add SQLAlchemy integration middleware to the FastAPI application.
fastapi_app.add_middleware(
    DBSessionMiddleware, db_url='sqlite:///:memory:'
)

# Build the middleware stack (happens automatically when the FastAPI app runs in a Web app server).
fastapi_app.build_middleware_stack()

# Define an adapter for the scoped session.
class FastapiScopedSession:
    def __getattribute__(self, item: str) -> None:
        return getattr(db.session, item)

# Construct an event-sourced application within a scoped session.
with db(commit_on_exit=True):
    # Produce the topic of the scoped session adapter class.
    scoped_session_adapter_topic = get_topic(FlaskScopedSession)
    # Construct event-sourced application to use scoped sessions.
    es_app = Application(
        env={"SQLALCHEMY_SCOPED_SESSION_TOPIC": get_topic(FastapiScopedSession)}
    )

# Create a new event-sourced aggregate.
with db(commit_on_exit=True):  # This happens automatically before handling a route.
    # Handle request.
    aggregate = Aggregate()
    es_app.save(aggregate)
    es_app.repository.get(aggregate.id)

# The aggregate has been committed.
with db(commit_on_exit=True):  # This happens automatically before handling a route.
    # Handle request.
    es_app.repository.get(aggregate.id)

# Raise exception after creating aggregate.
try:
    with db(commit_on_exit=True):
        # Handle request.
        aggregate = Aggregate()
        es_app.save(aggregate)
        es_app.repository.get(aggregate.id)
        raise TypeError("An error occurred!!!")
except TypeError:
    # Web framework returns an error.
    ...
else:
    raise Exception("Expected type error")

# The aggregate hasn't been committed.
with db(commit_on_exit=True):
    try:
        es_app.repository.get(aggregate.id)
    except AggregateNotFoundError:
        pass
    else:
        raise Exception("Expected aggregate not found")
```



## Google Cloud SQL Python Connector

You can set the environment variable `SQLALCHEMY_CONNECTION_CREATOR_TOPIC` to a topic
that will resolve to a callable that will be used to create database connections.

For example, you can use the [Cloud SQL Python Connector](https://pypi.org/project/cloud-sql-python-connector/)
in the following way.

First install the Cloud SQL Python Connector package from PyPI.

    $ pip install 'cloud-sql-python-connector[pg8000]'

Then define a `getconn()` function, following the advice in the Cloud SQL
Python Connector README page.

```python
from google.cloud.sql.connector import Connector

# initialize Connector object
connector = Connector()

# function to return the database connection
def get_google_cloud_sql_conn():
    return connector.connect(
        "project:region:instance",
        "pg8000",
        user="postgres-iam-user@gmail.com",
        db="my-db-name",
        enable_iam_auth=True,
   )
```

Set the environment variable `'SQLALCHEMY_CONNECTION_CREATOR_TOPIC'`, along with
`'PERSISTENCE_MODULE'` and `'SQLALCHEMY_URL'`.

```python
from eventsourcing.utils import get_topic

os.environ['PERSISTENCE_MODULE'] = 'eventsourcing_sqlalchemy'
os.environ['SQLALCHEMY_URL'] = 'postgresql+pg8000://'
os.environ['SQLALCHEMY_CONNECTION_CREATOR_TOPIC'] = get_topic(get_google_cloud_sql_conn)
```

## More information

See the library's [documentation](https://eventsourcing.readthedocs.io/)
and the [SQLAlchemy](https://www.sqlalchemy.org/) project for more information.
