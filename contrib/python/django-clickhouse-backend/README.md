Django ClickHouse Database Backend
===
[![PyPI - Version](https://img.shields.io/pypi/v/django-clickhouse-backend)](https://pypi.org/project/django-clickhouse-backend)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/django-clickhouse-backend)](https://pypi.org/project/django-clickhouse-backend)
[![PyPI django version](https://img.shields.io/pypi/frameworkversions/django/django-clickhouse-backend)](https://pypi.org/project/django-clickhouse-backend)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/django-clickhouse-backend)](https://pypi.org/project/django-clickhouse-backend)
[![GitHub licence](https://img.shields.io/github/license/jayvynl/django-clickhouse-backend)](https://github.com/jayvynl/django-clickhouse-backend/blob/main/LICENSE)
[![GitHub Action: Test](https://github.com/jayvynl/django-clickhouse-backend/actions/workflows/test-main.yml/badge.svg)](https://github.com/jayvynl/django-clickhouse-backend/actions/workflows/test-main.yml)
[![Coverage Status](https://coveralls.io/repos/github/jayvynl/django-clickhouse-backend/badge.svg?branch=main)](https://coveralls.io/github/jayvynl/django-clickhouse-backend?branch=main)
[![Code style](https://camo.githubusercontent.com/051a04ae958f4a1a5d6444df4cdc520305eef93d5028e6d4c7cd16efa3136cd4/68747470733a2f2f696d672e736869656c64732e696f2f656e64706f696e743f75726c3d68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f61737472616c2d73682f727566662f6d61696e2f6173736574732f62616467652f76322e6a736f6e)](https://github.com/astral-sh/ruff-pre-commit)

Django clickhouse backend is a [django database backend](https://docs.djangoproject.com/en/5.1/ref/databases/) for
[clickhouse](https://clickhouse.com/docs/en/home/) database. This project allows using django ORM to interact with
clickhouse, the goal of the project is to operate clickhouse like operating mysql, postgresql in django.

Thanks to [clickhouse driver](https://github.com/mymarilyn/clickhouse-driver), django clickhouse backend use it as [DBAPI](https://peps.python.org/pep-0249/).
Thanks to [clickhouse pool](https://github.com/ericmccarthy7/clickhouse-pool), it makes clickhouse connection pool.

Read [Documentation](https://github.com/jayvynl/django-clickhouse-backend/blob/main/docs/README.md) for more.

**Features:**

- Reuse most of the existed django ORM facilities, minimize your learning costs.
- Connect to clickhouse efficiently via [clickhouse native interface](https://clickhouse.com/docs/en/interfaces/tcp/) and connection pool.
- No other intermediate storage, no need to synchronize data, just interact directly with clickhouse.
- Support clickhouse specific schema features such as [Engine](https://clickhouse.com/docs/en/engines/table-engines/) and [Index](https://clickhouse.com/docs/en/guides/improving-query-performance/skipping-indexes).
- Support most types of table migrations.
- Support creating test database and table, working with django TestCase and pytest-django.
- Support most clickhouse data types.
- Support [SETTINGS in SELECT Query](https://clickhouse.com/docs/en/sql-reference/statements/select/#settings-in-select-query).
- Support [PREWHERE clause](https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere).
- Support query results returned in columns and [deserialized to `numpy` objects](https://clickhouse-driver.readthedocs.io/en/latest/features.html#numpy-pandas-support).

**Notes:**

- Not tested upon all versions of clickhouse-server, clickhouse-server 22.x.y.z or over is suggested.
- Aggregation functions result in 0 or nan (Not NULL) when data set is empty. max/min/sum/count is 0, avg/STDDEV_POP/VAR_POP is nan.
- In outer join, clickhouse will set missing columns to empty values (0 for number, empty string for text, unix epoch for date/datatime) instead of NULL.
  So Count("book") resolve to 1 in a missing LEFT OUTER JOIN match, not 0.
  In aggregation expression Avg("book__rating", default=2.5), default=2.5 have no effect in a missing match.
- Clickhouse does not support unique constraint and foreignkey constraint. `ForeignKey`, `ManyToManyField` and `OneToOneField` can be used with clickhouse backend, but no database level constraints will be added, so there could be some consistency problems.
- Clickhouse does not support transaction. If any exception occurs during migrating, then your clickhouse database will be in an untracked state. Any migration should be full tested in test environment before deployed to production environment.
- This project does not support migrations of changing table engine and settings yet.

**Requirements:**

- [Python](https://www.python.org/) >= 3.7
- [Django](https://docs.djangoproject.com/) >= 3.2
- [clickhouse driver](https://github.com/mymarilyn/clickhouse-driver)
- [clickhouse pool](https://github.com/ericmccarthy7/clickhouse-pool)


Get started
---

### Installation

```shell
$ pip install django-clickhouse-backend
```

or

```shell
$ git clone https://github.com/jayvynl/django-clickhouse-backend
$ cd django-clickhouse-backend
$ python setup.py install
```

### Configuration


Only `ENGINE` is required in database setting, other options have default values.

- ENGINE: required, set to `clickhouse_backend.backend`.
- NAME: database name, default `default`.
- HOST: database host, default `localhost`.
- PORT: database port, default `9000`.
- USER: database user, default `default`.
- PASSWORD: database password, default empty.

In the most cases, you may just use clickhouse to store some big events tables,
and use some RDBMS to store other tables.
Here I give an example setting for clickhouse and postgresql.

```python
INSTALLED_APPS = [
    # ...
    "clickhouse_backend",
    # ...
]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "localhost",
        "USER": "postgres",
        "PASSWORD": "123456",
        "NAME": "postgres",
    },
    "clickhouse": {
        "ENGINE": "clickhouse_backend.backend",
        "NAME": "default",
        "HOST": "localhost",
        "USER": "DB_USER",
        "PASSWORD": "DB_PASSWORD",
    }
}
DATABASE_ROUTERS = ["dbrouters.ClickHouseRouter"]
```

```python
# dbrouters.py
from clickhouse_backend.models import ClickhouseModel


def get_subclasses(class_):
    classes = class_.__subclasses__()

    index = 0
    while index < len(classes):
        classes.extend(classes[index].__subclasses__())
        index += 1

    return list(set(classes))


class ClickHouseRouter:
    def __init__(self):
        self.route_model_names = set()
        for model in get_subclasses(ClickhouseModel):
            if model._meta.abstract:
                continue
            self.route_model_names.add(model._meta.label_lower)

    def db_for_read(self, model, **hints):
        if (model._meta.label_lower in self.route_model_names
                or hints.get("clickhouse")):
            return "clickhouse"
        return None

    def db_for_write(self, model, **hints):
        if (model._meta.label_lower in self.route_model_names
                or hints.get("clickhouse")):
            return "clickhouse"
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if (f"{app_label}.{model_name}" in self.route_model_names
                or hints.get("clickhouse")):
            return db == "clickhouse"
        elif db == "clickhouse":
            return False
        return None
```

You should use [database router](https://docs.djangoproject.com/en/5.1/topics/db/multi-db/#automatic-database-routing) to
automatically route your queries to the right database. In the preceding example, I write a database router which route all
queries from subclasses of `clickhouse_backend.models.ClickhouseModel` or custom migrations with a `clickhouse` hint key to clickhouse.
All other queries are routed to the default database (postgresql).


### Model Definition

Clickhouse backend support django builtin fields and clickhouse specific fields.

Read [fields documentation](https://github.com/jayvynl/django-clickhouse-backend/blob/main/docs/Fields.md) for more.

Notices about model definition:

- import models from clickhouse_backend, not from django.db
- add low_cardinality for StringFiled, when the data field cardinality is relatively low, this configuration can significantly improve query performance

- cannot use db_index=True in Field, but we can add in the Meta indexes
- need to specify the ordering in Meta just for default query ordering
- need to specify the engine for clickhouse, specify the order_by for clickhouse order and the partition_by argument

```python
from django.db.models import CheckConstraint, IntegerChoices, Q
from django.utils import timezone

from clickhouse_backend import models


class Event(models.ClickhouseModel):
    class Action(IntegerChoices):
        PASS = 1
        DROP = 2
        ALERT = 3
    ip = models.GenericIPAddressField(default="::")
    ipv4 = models.IPv4Field(default="127.0.0.1")
    ip_nullable = models.GenericIPAddressField(null=True)
    port = models.UInt16Field(default=0)
    protocol = models.StringField(default="", low_cardinality=True)
    content = models.StringField(default="")
    timestamp = models.DateTime64Field(default=timezone.now)
    created_at = models.DateTime64Field(auto_now_add=True)
    action = models.EnumField(choices=Action.choices, default=Action.PASS)

    class Meta:
        ordering = ["-timestamp"]
        engine = models.MergeTree(
            primary_key="timestamp",
            order_by=("timestamp", "id"),
            partition_by=models.toYYYYMMDD("timestamp"),
            index_granularity=1024,
            index_granularity_bytes=1 << 20,
            enable_mixed_granularity_parts=1,
        )
        indexes = [
            models.Index(
                fields=["ip"],
                name="ip_set_idx",
                type=models.Set(1000),
                granularity=4
            ),
            models.Index(
                fields=["ipv4"],
                name="ipv4_bloom_idx",
                type=models.BloomFilter(0.001),
                granularity=1
            )
        ]
        constraints = (
            CheckConstraint(
                name="port_range",
                check=Q(port__gte=0, port__lte=65535),
            ),
        )
```

### Migration

```shell
$ python manage.py makemigrations
```

this operation will generate migration file under apps/migrations/

then we mirgrate

```shell
$ python manage.py migrate --database clickhouse
```

for the first time run, this operation will generate django_migrations table with create table sql like this

```sql
> show create table django_migrations;

CREATE TABLE other.django_migrations
(
    `id` Int64,
    `app` FixedString(255),
    `name` FixedString(255),
    `applied` DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192 
```

we can query it with results like this

```sql
> select * from django_migrations;

┌──────────────────id─┬─app─────┬─name─────────┬────────────────────applied─┐
│ 1626937818115211264 │ testapp │ 0001_initial │ 2023-02-18 13:32:57.538472 │
└─────────────────────┴─────────┴──────────────┴────────────────────────────┘

```

migrate will create a table with name event as we define in the models

```sql
> show create table event;

CREATE TABLE other.event
(
    `id` Int64,
    `ip` IPv6,
    `ipv4` IPv6,
    `ip_nullable` Nullable(IPv6),
    `port` UInt16,
    `protocol` LowCardinality(String),
    `content` String,
    `timestamp` DateTime64(6, 'UTC'),
    `created_at` DateTime64(6, 'UTC'),
    `action` Enum8('Pass' = 1, 'Drop' = 2, 'Alert' = 3),
    INDEX ip_set_idx ip TYPE set(1000) GRANULARITY 4,
    INDEX port_bloom_idx port TYPE bloom_filter(0.001) GRANULARITY 1,
    CONSTRAINT port_range CHECK (port >= 0) AND (port <= 65535)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY id
SETTINGS index_granularity = 8192
```

### Operate Data

create

```python
for i in range(10):
    Event.objects.create(ip_nullable=None, port=i,
                         protocol="HTTP", content="test",
                         action=Event.Action.PASS.value)
assert Event.objects.count() == 10
```

query

```python
queryset = Event.objects.filter(content="test")
for i in queryset:
    print(i)
```

update

```python
Event.objects.filter(port__in=[1, 2, 3]).update(protocol="TCP")
time.sleep(1)
assert Event.objects.filter(protocol="TCP").count() == 3
```

delete

```python
Event.objects.filter(protocol="TCP").delete()
time.sleep(1)
assert not Event.objects.filter(protocol="TCP").exists()
```

Except for the model definition, all other operations are like operating relational databases such as mysql and postgresql

### Testing

Writing testcase is all the same as normal django project. You can use django TestCase or pytest-django.
**Notice:** clickhouse use mutations for [deleting or updating](https://clickhouse.com/docs/en/guides/developer/mutations).
By default, data mutations is processed asynchronously.
That is, when you update or delete a row, clickhouse will perform the action after a period of time.
So you should change this default behavior in testing for deleting or updating.
There are 2 ways to do that:

- Config database engine as follows, this sets [`mutations_sync=1`](https://clickhouse.com/docs/en/operations/settings/settings#mutations_sync) at session scope.
  ```python
  DATABASES = {
      "default": {
          "ENGINE": "clickhouse_backend.backend",
          "OPTIONS": {
              "settings": {
                  "mutations_sync": 1,
              }
          }
      }
  }
  ```
- Use [SETTINGS in SELECT Query](https://clickhouse.com/docs/en/sql-reference/statements/select/#settings-in-select-query).
  ```python
  Event.objects.filter(protocol="UDP").settings(mutations_sync=1).delete()
  ```

Sample test case.

```python
from django.test import TestCase


class TestEvent(TestCase):
    databases = {"default", "clickhouse"}

    def test_spam(self):
        assert Event.objects.count() == 0
```

Distributed table
---

This backend support [distributed DDL queries (ON CLUSTER clause)](https://clickhouse.com/docs/en/sql-reference/distributed-ddl)
and [distributed table engine](https://clickhouse.com/docs/en/engines/table-engines/special/distributed).

The following example assumes that a cluster defined by [docker compose in this repository](https://github.com/jayvynl/django-clickhouse-backend/blob/main/compose.yaml) is used.
This cluster name is `cluster`, it has 2 shards, every shard has 2 replica.

Query results returned as columns and/or deserialized into `numpy` objects 
---

`clickhouse-driver` allows results to be returned as columns and/or deserialized into
`numpy` objects. This backend supports both options by using the context manager, 
`Cursor.set_query_execution_args()`.

```python
import numpy as np
from django.db import connection

sql = """
    SELECT toDateTime32('2022-01-01 01:00:05', 'UTC'), number, number*2.5
    FROM system.numbers
    LIMIT 3
"""
with connection.cursor() as cursorWrapper:
    with cursorWrapper.cursor.set_query_execution_args(
        columnar=True, use_numpy=True
    ) as cursor:
        cursor.execute(sql)
        np.testing.assert_equal(
            cursor.fetchall(),
            [
                np.array(
                    [
                        np.datetime64("2022-01-01T01:00:05"),
                        np.datetime64("2022-01-01T01:00:05"),
                        np.datetime64("2022-01-01T01:00:05"),
                    ],
                    dtype="datetime64[s]",
                ),
                np.array([0, 1, 2], dtype=np.uint64),
                np.array([0, 2.5, 5.0], dtype=np.float64),
            ],
        )

        cursor.execute(sql)
        np.testing.assert_equal(
            cursor.fetchmany(2),
            [
                np.array(
                    [
                        np.datetime64("2022-01-01T01:00:05"),
                        np.datetime64("2022-01-01T01:00:05"),
                        np.datetime64("2022-01-01T01:00:05"),
                    ],
                    dtype="datetime64[s]",
                ),
                np.array([0, 1, 2], dtype=np.uint64),
            ],
        )
```

### Configuration

```python
DATABASES = {
    "default": {
        "ENGINE": "clickhouse_backend.backend",
        "OPTIONS": {
            "migration_cluster": "cluster",
            "settings": {
                "mutations_sync": 2,
                "insert_distributed_sync": 1,
                "insert_quorum": 2,
                "alter_sync": 2,
            },
        },
        "TEST": {"cluster": "cluster"},
    },
    "s1r2": {
        "ENGINE": "clickhouse_backend.backend",
        "PORT": 9001,
        "OPTIONS": {
            "migration_cluster": "cluster",
            "settings": {
                "mutations_sync": 2,
                "insert_distributed_sync": 1,
                "insert_quorum": 2,
                "alter_sync": 2,
            },
        },
        "TEST": {"cluster": "cluster", "managed": False, "DEPENDENCIES": ["default"]},
    },
    "s2r1": {
        "ENGINE": "clickhouse_backend.backend",
        "PORT": 9002,
        "OPTIONS": {
            "migration_cluster": "cluster",
            "settings": {
                "mutations_sync": 2,
                "insert_distributed_sync": 1,
                "insert_quorum": 2,
                "alter_sync": 2,
            },
        },
        "TEST": {"cluster": "cluster", "managed": False, "DEPENDENCIES": ["default"]},
    },
    "s2r2": {
        "ENGINE": "clickhouse_backend.backend",
        "PORT": 9003,
        "OPTIONS": {
            "migration_cluster": "cluster",
            "settings": {
                "mutations_sync": 2,
                "insert_distributed_sync": 1,
                "insert_quorum": 2,
                "alter_sync": 2,
            },
        },
        "TEST": {"cluster": "cluster", "managed": False, "DEPENDENCIES": ["default"]},
    },
}
```

Extra settings explanation:

- `"migration_cluster": "cluster"`
  Migration table will be created on this cluster if this setting is specified, otherwise only local migration table is created.
- `"mutations_sync": 2`
  This is suggested if you want to test [data mutations](https://clickhouse.com/docs/en/guides/developer/mutations) on replicated table.
  *Don't* set this in production environment.
- `"insert_distributed_sync": 1`
  This is suggested if you want to test inserting data into distributed table.
  *Don't* set this in production environment.
- `"insert_quorum": 2`
  This is suggested if you want to test inserting data into replicated table.
  The value is set to replica number.
- `"alter_sync": 2`
  This is suggested if you want to test altering or truncating replicated table.
  *Don't* set this in production environment.
- `"TEST": {"cluster": "cluster", "managed": False, "DEPENDENCIES": ["default"]}`
  Test database will be created on this cluster.
  If you have multiple database connections to the same cluster and want to run tests over all these connections,
  then only one connection should set `"managed": True`(the default value), other connections should set `"managed": False`.
  So that test database will not be created multiple times.
  
  If your managed database alias is `s1r2` instead `default`, `"DEPENDENCIES": ["s1r2"]` should be set to ensure the [creation order for test databases](https://docs.djangoproject.com/en/4.2/topics/testing/advanced/#controlling-creation-order-for-test-databases).

  Do not hardcode database name when you define replicated table or distributed table.
  Because test database name is different from deployed database name.

#### Clickhouse cluster behind a load balancer

If your clickhouse cluster is running behind a load balancer, you can optionally set `distributed_migrations` to `True` under database OPTIONS. 
Then a distributed migration table will be created on all nodes of the cluster, and all migration operations will be performed on this 
distributed migrations table instead of a local migrations table. Otherwise, sequentially running migrations will have no effect on other nodes.

Configuration example:

```python
DATABASES = {
    "default": {
        "HOST": "clickhouse-load-balancer",
        "PORT": 9000,
        "ENGINE": "clickhouse_backend.backend",
        "OPTIONS": {
            "migration_cluster": "cluster",
            "distributed_migrations": True,
            "settings": {
                "mutations_sync": 2,
                "insert_distributed_sync": 1,
                "insert_quorum": 2,
                "alter_sync": 2,
            },
        },
    }
}
```

### Model

`cluster` in `Meta` class will make models being created on cluster.

```python
from clickhouse_backend import models


class Student(models.ClickhouseModel):
    name = models.StringField()
    address = models.StringField()
    score = models.Int8Field()

    class Meta:
        engine = models.ReplicatedMergeTree(
            "/clickhouse/tables/{uuid}/{shard}",
            # Or if you want to use database name or table name, you should also use macro instead of hardcoded name.
            # "/clickhouse/tables/{database}/{table}/{shard}",
            "{replica}",
            order_by="id"
        )
        cluster = "cluster"


class DistributedStudent(models.ClickhouseModel):
    name = models.StringField()
    score = models.Int8Field()

    class Meta:
        engine = models.Distributed(
            "cluster", models.currentDatabase(), Student._meta.db_table, models.Rand()
        )
        cluster = "cluster"
```

### CRUD

Just like normal table, you can do whatever you like to distributed table.

```python
students = DistributedStudent.objects.bulk_create([DistributedStudent(name=f"Student{i}", score=i * 10) for i in range(10)])
assert DistributedStudent.objects.count() == 10
DistributedStudent.objects.filter(id__in=[s.id for s in students[5:]]).update(name="lol")
DistributedStudent.objects.filter(id__in=[s.id for s in students[:5]]).delete()
```

### Migrate

If `migration_cluster` is not specified in database configuration. You should always run migrating on one specific cluster node.
Because other nodes do not know whether migrations have been applied by any other node.

If `migration_cluster` is specified. Then migration table(named `django_migrations`) will be created on the specified cluster.
When applied, [migration operations](https://docs.djangoproject.com/en/4.2/ref/migration-operations/) of model with cluster defined in `Meta` class
will be executed on cluster, other migration operations will be executed locally.
This means distributed table will be created on all nodes as long as any node has applied the migrations.
Other local table will only be created on node which has applied the migrations.

If you want to use local table in all nodes, you should apply migrations multiple times on all nodes.
But remember, these local tables store data separately, currently this backend do not provide means to query data from other nodes.

```shell
python manage.py migrate
python manage.py migrate --database s1r2
python manage.py migrate --database s2r1
python manage.py migrate --database s2r2
```

### Update

When updated from django clickhouse backend 1.1.0 or lower, you should not add cluster related settings to your
existing project. Because:

- Migration table schema won't be changed if you add or remove `migration_on_cluster`. And `python mange.py migrate` will work abnormally.
- If you add cluster to your existing model's `Meta` class, no schema changes will occur, this project does not support this yet.

If you really want to use cluster feature with existing project, you should manage schema changes yourself.
These steps should be tested carefully in test environment.
[Clickhouse docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication#converting-from-mergetree-to-replicatedmergetree) may be helpful.

1. Apply all your existing migrations.
2. Change your settings and model.
3. Generate new migrations.
4. Log into your clickhouse database and change table schemas to reflect your models.
5. Apply migrations with fake flag.

```shell
python manage.py migrate
# Change your settings and model
python manage.py makemigrations
# Log into your clickhouse database and change table schemas to reflect your models.
python manage.py migrate --fake
```


Test
---

To run test for this project:

```shell
$ git clone https://github.com/jayvynl/django-clickhouse-backend
$ cd django-clickhouse-backend
# docker and docker-compose are required.
$ docker-compose up -d
$ python tests/runtests.py
# run test for every python version and django version
$ pip install tox
$ tox
```

Changelog
---

[All changelogs](https://github.com/jayvynl/django-clickhouse-backend/blob/main/CHANGELOG.md).

Contributing
---

Read [Contributing guide](./CONTRIBUTING.md).

License
---

Django clickhouse backend is distributed under the [MIT license](http://www.opensource.org/licenses/mit-license.php).
