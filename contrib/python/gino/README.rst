======
|GINO|
======

`ВНИМАНИЕ`
В аркадии лежит пропатченная версия, обновляйте осторожно.
В чем отличие - вместе стандартного pool из asyncpg можно использовать
pool из библиотеки pyscopg2 https://a.yandex-team.ru/arc/trunk/arcadia/library/python/pyscopg2 , так как
стандартный не знает ничего про мастера/реплику и (на текущий момент) не умеет переживать их переключение, а
использование cname, которые предоставляет MDB - не является надежным решением
так как при переключении мастера ваш сервис несколько минут будет в read-only пока
DNS не обновится.
Как работает - при использовании Pyscopg2Pool - get запросы автоматически будут уходить
на мастер, а create/update/delete - на реплики (если они есть и доступны).
При этом можно явно сказать о том, что прочитать нужно с мастера - вот так
:code:`domain = await Domain.query.gino.first(read_only=False)`

Сделать аналогичный ПР в gino на гитхабе пока нельзя, так как pyscopg2 только
планируют выложить в opensource.

Пример использования:

.. code-block:: python

    from gino.ext.starlette import Gino
    from sqlalchemy.engine.url import URL
    from gino.dialects.asyncpg import Pyscopg2Pool


    def get_engine_config():
           return {
            'dsn': URL(
                drivername=config.db_drivername,
                username=config.db_username,
                password=config.db_password,
                host=config.db_host, # строка вида 'master-host,replica-host-1,replica-host-2'
                port=config.db_port,
                database=config.db_name,
            ),
            'ssl': config.db_ssl,
            'echo': config.db_echo,
            'pool_min_size': config.db_pool_min_size,
            'pool_max_size': config.db_pool_max_size,
            'use_connection_for_request': config.db_use_connection_for_request,
            'retry_limit': config.db_retry_limit,
            'retry_interval': config.db_retry_interval,
            'kwargs': {'pool_class': Pyscopg2Pool, }, # вот тут нужно передать новый класс Pool
        }


    db = Gino(**get_engine_config())

GINO - GINO Is Not ORM - is a lightweight asynchronous ORM built on top of
SQLAlchemy_ core for Python asyncio_. GINO 1.0 supports only PostgreSQL_ with asyncpg_.

* Free software: BSD license
* Requires: Python 3.5
* GINO is developed proudly with |PyCharm|.


Home
----

`python-gino.org <https://python-gino.org/>`__


Documentation
-------------

* English_
* Chinese_


Installation
------------

.. code-block:: console

    $ pip install gino


Features
--------

* Robust SQLAlchemy-asyncpg bi-translator with no hard hack
* Asynchronous SQLAlchemy-alike engine and connection
* Asynchronous dialect API
* Asynchronous-friendly CRUD objective models
* Well-considered contextual connection and transaction management
* Reusing native SQLAlchemy core to build queries with grammar sugars
* Support SQLAlchemy ecosystem, e.g. Alembic_ for migration
* `Community support <https://github.com/python-gino/>`_ for Starlette_/FastAPI_, aiohttp_, Sanic_, Tornado_ and Quart_
* Rich PostgreSQL JSONB support

.. role:: red
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _asyncpg: https://github.com/MagicStack/asyncpg
.. _PostgreSQL: https://www.postgresql.org/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _Alembic: https://bitbucket.org/zzzeek/alembic
.. _Sanic: https://github.com/channelcat/sanic
.. _Tornado: http://www.tornadoweb.org/
.. _Quart: https://gitlab.com/pgjones/quart/
.. _English: https://python-gino.org/docs/en/
.. _Chinese: https://python-gino.org/docs/zh/
.. _aiohttp: https://github.com/aio-libs/aiohttp
.. _Starlette: https://www.starlette.io/
.. _FastAPI: https://fastapi.tiangolo.com/
.. |PyCharm| image:: ./docs/images/pycharm.svg
        :height: 20px
        :target: https://www.jetbrains.com/?from=GINO

.. |GINO| image:: ./docs/theme/static/logo.svg
        :alt: GINO
        :height: 64px
        :target: https://python-gino.org/
