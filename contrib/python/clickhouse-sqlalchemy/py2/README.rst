ClickHouse SQLAlchemy
=====================

ClickHouse dialect for SQLAlchemy to `ClickHouse database <https://clickhouse.yandex/>`_.


.. image:: https://img.shields.io/pypi/v/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://coveralls.io/repos/github/xzkostyan/clickhouse-sqlalchemy/badge.svg?branch=master
    :target: https://coveralls.io/github/xzkostyan/clickhouse-sqlalchemy?branch=master

.. image:: https://img.shields.io/pypi/l/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://img.shields.io/pypi/pyversions/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://img.shields.io/pypi/dm/clickhouse-sqlalchemy.svg
    :target: https://pypi.org/project/clickhouse-sqlalchemy

.. image:: https://github.com/xzkostyan/clickhouse-sqlalchemy/actions/workflows/actions.yml/badge.svg
   :target: https://github.com/xzkostyan/clickhouse-sqlalchemy/actions/workflows/actions.yml


Documentation
=============

Documentation is available at https://clickhouse-sqlalchemy.readthedocs.io.


Usage
=====

Supported interfaces:

- **native** [recommended] (TCP) via `clickhouse-driver <https://github.com/mymarilyn/clickhouse-driver>`_
- **http** via requests

Define table

    .. code-block:: python

        from sqlalchemy import create_engine, Column, MetaData

        from clickhouse_sqlalchemy import (
            Table, make_session, get_declarative_base, types, engines
        )

        uri = 'clickhouse+native://localhost/default'

        engine = create_engine(uri)
        session = make_session(engine)
        metadata = MetaData(bind=engine)

        Base = get_declarative_base(metadata=metadata)

        class Rate(Base):
            day = Column(types.Date, primary_key=True)
            value = Column(types.Int32)

            __table_args__ = (
                engines.Memory(),
            )

        Rate.__table__.create()


Insert some data

    .. code-block:: python

        from datetime import date, timedelta

        from sqlalchemy import func

        today = date.today()
        rates = [
            {'day': today - timedelta(i), 'value': 200 - i}
            for i in range(100)
        ]


And query inserted data

    .. code-block:: python

        session.execute(Rate.__table__.insert(), rates)

        session.query(func.count(Rate.day)) \
            .filter(Rate.day > today - timedelta(20)) \
            .scalar()


License
=======

ClickHouse SQLAlchemy is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.
