import json
import pytest
import logging
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
import ydb.issues
import os
from collections import Counter
from itertools import chain, islice

import random

# TODO:
# Mostly identical to yqv1 test ydb/tests/fq/generic/streaming/test_json.py
# Keep in sync (until yqv1 will be decomissioned)

USER_TOKEN = "root@builtin"

MAX_WRITE_STREAM_SIZE = 500
DEBUG = 0
SEED = 0  # use fixed seed for regular tests
if DEBUG:
    if "RANDOM_SEED" in os.environ:
        SEED = int(os.environ["RANDOM_SEED"])
    else:
        SEED = random.randint(0, (1 << 31))
        logging.debug(f"RANDOM_SEED={SEED}")
random.seed(SEED)


def ResequenceId(messages, field="id"):
    res = []
    i = 1
    for pair in messages:
        rpair = []
        for it in pair:
            src = json.loads(it)
            if field in src:
                src[field] = i
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
        i += 1
    return res


def freeze(obj):
    # Designed for (deserialized) json
    t = type(obj)
    if t == dict:
        return frozenset((k, freeze(v)) for k, v in obj.items())
    if t == list:
        return tuple(map(freeze, obj))
    return obj


def create_secret(kikimr: Kikimr, secret_name: str) -> None:
    kikimr.ydb_client.query(f"""
        CREATE SECRET `{secret_name}` WITH (value="{USER_TOKEN}");
    """)


def create_source(
    kikimr: Kikimr,
    source_name: str,
    secret_path: str,
    endpoint: Endpoint,
    shared_reading: bool = False,
) -> None:
    """Create an External Data Source that authenticates via IAM."""
    kikimr.ydb_client.query(f"""
        CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
            SOURCE_TYPE = "Ydb",
            LOCATION = "{endpoint.endpoint}",
            DATABASE_NAME = "{endpoint.database}",
            USE_TLS = "FALSE",
            AUTH_METHOD = "TOKEN",
            TOKEN_SECRET_PATH = "{secret_path}",
            SHARED_READING="{shared_reading}"
        );
    """)


def create_table(
    kikimr: Kikimr,
    column_tables: bool,
) -> None:
    pknull = 'NOT NULL' if column_tables else ''
    with_store = ' WITH (STORE=COLUMN)' if column_tables else ''
    kikimr.ydb_client.query(f"""
    CREATE TABLE simple_table (number Int32 {pknull}, PRIMARY KEY (number)){with_store};
    CREATE TABLE join_table (id Int32 {pknull}, data STRING, PRIMARY KEY (id)){with_store};
    CREATE TABLE users (age Int32, id Int32 {pknull}, ip STRING, name STRING, region Int32, PRIMARY KEY(id)){with_store};
    CREATE TABLE db (
        b STRING NOT NULL,
        c Uint32,
        a Int32 NOT NULL,
        d Int8,
        f Int32,
        e Int64,
        g Int32,
        h Int32,
        is_odd Bool NOT NULL,
        is_true Bool NOT NULL,
        is_false Bool NOT NULL,
        opt_odd Bool,
        opt_true Bool,
        opt_false Bool,
        opt_null Bool,
        ts Timestamp,
        dur Interval,
        tsd Date,
        PRIMARY KEY(b, a));
    """)
    kikimr.ydb_client.query("""
    INSERT INTO simple_table (number) VALUES
      (1),
      (2),
      (3);
    INSERT INTO join_table (id, data) VALUES
      (1, "ydb10"),
      (2, "ydb20"),
      (3, "ydb30");
    INSERT INTO users (age, id, ip, name, region) VALUES
      (15, 1, "95.106.17.32", "Anya", 213),
      (25, 2, "88.78.248.151", "Petr", 225),
      (17, 3, "93.94.183.63", "Masha", 1),
      (5, 4, "::ffff:193.34.173.188", "Alena", 225),
      (15, 5, "93.170.111.29", "Irina", 2),
      (13, 6, "93.170.111.28", "Inna", 21),
      (33, 7, "::ffff:193.34.173.173", "Ivan", 125),
      (45, 8, "::ffff:133.34.173.188", "Asya", 225),
      (27, 9, "::ffff:133.34.172.188", "German", 125),
      (41, 10, "::ffff:133.34.173.185", "Olya", 225),
      (35, 11, "::ffff:193.34.163.188", "Slava", 2),
      (56, 12, "2a02:1812:1713:4f00:517e:1d79:c88b:704", "Elena", 2),
      (18, 17, "ivalid ip", "newUser", 12);
    INSERT INTO db (a, b, c, d, e, f, is_odd, is_true, is_false, opt_odd, opt_true, opt_false, opt_null, ts, dur, tsd) VALUES
      (1, "2", 3, 4, 5, 6, true, true, false, true, true, false, NULL, Timestamp("1970-01-03T10:11:12Z"), Interval("PT13S"), Date("1970-01-05")),
      (7, "8", 9, 10, 11, 12, true, true, false, true, true, false, NULL, Timestamp("1970-01-03T10:11:13Z"), Interval("PT14S"), Date("1970-01-06")),
      (2, "3", 6, NULL, 8, 9, false, true, false, false, true, false, NULL, Timestamp("1970-01-03T10:11:14Z"), Interval("PT15S"), Date("1970-01-07")),
      (4, "5", 4, 15, 17, NULL, false, true, false, false, true, false, NULL, Timestamp("1970-01-03T10:11:15Z"), Interval("PT16S"), Date("1970-01-08"));
    """)


TESTCASES = [
    # 0
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`;

            $enriched = select
                            e.Data as data, u.id as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(e.Data = u.data)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            ('ydb10', '{"data":"ydb10","lookup":1}'),
            ('ydb20', '{"data":"ydb20","lookup":2}'),
            ('ydb30', '{"data":"ydb30","lookup":3}'),
            ('ydb40', '{"data":"ydb40","lookup":null}'),
            ('ydb50', '{"data":"ydb50","lookup":null}'),
            ('ydb10', '{"data":"ydb10","lookup":1}'),
            ('ydb20', '{"data":"ydb20","lookup":2}'),
            ('ydb30', '{"data":"ydb30","lookup":3}'),
            ('ydb40', '{"data":"ydb40","lookup":null}'),
            ('ydb50', '{"data":"ydb50","lookup":null}'),
        ]
        * 10,
    ),
    # 1
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`;

            $enriched = select
                            e.Data as data, CAST(e.Data AS Int32) as id, u.data as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(CAST(e.Data AS Int32) = u.id)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            ('1', '{"data":"1","id":1,"lookup":"ydb10"}'),
            ('2', '{"data":"2","id":2,"lookup":"ydb20"}'),
            ('3', '{"data":"3","id":3,"lookup":"ydb30"}'),
            ('4', '{"data":"4","id":4,"lookup":null}'),
            ('5', '{"data":"5","id":5,"lookup":null}'),
            ('1', '{"data":"1","id":1,"lookup":"ydb10"}'),
            ('2', '{"data":"2","id":2,"lookup":"ydb20"}'),
            ('3', '{"data":"3","id":3,"lookup":"ydb30"}'),
            ('4', '{"data":"4","id":4,"lookup":null}'),
            ('5', '{"data":"5","id":5,"lookup":null}'),
        ]
        * 3,
    ),
    # 2
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            user Int32,
                        )
                    )            ;

            $enriched = select e.id as id,
                            e.user as user_id,
                            u.data as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(e.user = u.id)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                ('{"id":3,"user":5}', '{"id":3,"user_id":5,"lookup":null}'),
                ('{"id":9,"user":3}', '{"id":9,"user_id":3,"lookup":"ydb30"}'),
                ('{"id":2,"user":2}', '{"id":2,"user_id":2,"lookup":"ydb20"}'),
                ('{"id":1,"user":1}', '{"id":1,"user_id":1,"lookup":"ydb10"}'),
                ('{"id":10,"user":null}', '{"id":10,"user_id":null,"lookup":null}'),
                ('{"id":4,"user":3}', '{"id":4,"user_id":3,"lookup":"ydb30"}'),
                ('{"id":5,"user":3}', '{"id":5,"user_id":3,"lookup":"ydb30"}'),
                ('{"id":6,"user":1}', '{"id":6,"user_id":1,"lookup":"ydb10"}'),
                ('{"id":7,"user":2}', '{"id":7,"user_id":2,"lookup":"ydb20"}'),
            ]
            * 20
        ),
    ),
    # 3
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $formatTime = DateTime::Format("%H:%M:%S");

            $enriched = select e.id as id,
                            $formatTime(DateTime::ParseIso8601(e.ts)) as ts,
                            e.user as user_id,
                            u.data as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(e.user = u.id)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":2,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                    '{"id":2,"ts":"11:33:44","user_id":2,"lookup":"ydb20"}',
                ),
                (
                    '{"id":1,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                    '{"id":1,"ts":"11:22:33","user_id":1,"lookup":"ydb10"}',
                ),
                (
                    '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":5}',
                    '{"id":3,"ts":"11:33:55","user_id":5,"lookup":null}',
                ),
                (
                    '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                    '{"id":4,"ts":"11:33:56","user_id":3,"lookup":"ydb30"}',
                ),
                (
                    '{"id":5,"ts":"20240701T113357","ev_type":"foo5","user":3}',
                    '{"id":5,"ts":"11:33:57","user_id":3,"lookup":"ydb30"}',
                ),
                (
                    '{"id":6,"ts":"20240701T112238","ev_type":"foo6","user":1}',
                    '{"id":6,"ts":"11:22:38","user_id":1,"lookup":"ydb10"}',
                ),
                (
                    '{"id":7,"ts":"20240701T113349","ev_type":"foo7","user":2}',
                    '{"id":7,"ts":"11:33:49","user_id":2,"lookup":"ydb20"}',
                ),
            ]
            * 10
        ),
    ),
    # 4
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $formatTime = DateTime::Format("%H:%M:%S");

            $enriched = select e.id as id,
                            $formatTime(DateTime::ParseIso8601(e.ts)) as ts,
                            e.user as user_id,
                            u.id as uid,
                            u.name as name,
                            u.age as age
                from
                    $input as e
                left join {streamlookup} any {table_source}`users` as u
                on(e.user = u.id)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                    '{"id":1,"ts":"11:33:44","uid":2,"user_id":2,"name":"Petr","age":25}',
                ),
                (
                    '{"id":2,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                    '{"id":2,"ts":"11:22:33","uid":1,"user_id":1,"name":"Anya","age":15}',
                ),
                (
                    '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":100}',
                    '{"id":3,"ts":"11:33:55","uid":null,"user_id":100,"name":null,"age":null}',
                ),
                (
                    '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                    '{"id":4,"ts":"11:33:56","uid":3,"user_id":3,"name":"Masha","age":17}',
                ),
                (
                    '{"id":5,"ts":"20240701T113357","ev_type":"foo5","user":3}',
                    '{"id":5,"ts":"11:33:57","uid":3,"user_id":3,"name":"Masha","age":17}',
                ),
                (
                    '{"id":6,"ts":"20240701T112238","ev_type":"foo6","user":1}',
                    '{"id":6,"ts":"11:22:38","uid":1,"user_id":1,"name":"Anya","age":15}',
                ),
                (
                    '{"id":7,"ts":"20240701T113349","ev_type":"foo7","user":2}',
                    '{"id":7,"ts":"11:33:49","uid":2,"user_id":2,"name":"Petr","age":25}',
                ),
            ]
            * 1000
        ),
        "TTL",
        "10",
        "MaxCachedRows",
        "5",
        "MaxDelayedRows",
        "100",
        "ShuffleMode",
        "Hash",
        "FullscanLimit",
        "0",
    ),
    # 5
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $enriched = select e.id as id,
                            e.user as user_id,
                            eu.id as uid
                from
                    $input as e
                left join {streamlookup} any {table_source}`users` as eu
                on(e.user = eu.id)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            (
                '{"id":1,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                '{"id":1,"uid":2,"user_id":2}',
            ),
            (
                '{"id":2,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                '{"id":2,"uid":1,"user_id":1}',
            ),
            (
                '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":100}',
                '{"id":3,"uid":null,"user_id":100}',
            ),
            (
                '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                '{"id":4,"uid":3,"user_id":3}',
            ),
        ],
    ),
    # 6
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            yb STRING,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} any {table_source}db as u
                on(e.yb = u.b AND e.za = u.a )
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"yb":"2","yc":100,"zd":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"e":5,"f":6,"za":1,"yb":"2","yc":100,"zd":101}',
                ),
                (
                    '{"id":2,"za":7,"yb":"8","yc":106,"zd":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"e":11,"f":12,"za":7,"yb":"8","yc":106,"zd":107}',
                ),
                (
                    '{"id":3,"za":2,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":"1","yc":114,"zd":115}',
                ),
                (
                    '{"id":3,"za":2,"yb":null,"yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":null,"yc":114,"zd":115}',
                ),
            ]
        ),
        "FullscanLimit",
        "2",
    ),
    # 7
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            yb STRING,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} any {table_source}db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"yb":"2","yc":100,"zd":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"e":5,"f":6,"za":1,"yb":"2","yc":100,"zd":101}',
                ),
                (
                    '{"id":2,"za":7,"yb":"8","yc":106,"zd":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"e":11,"f":12,"za":7,"yb":"8","yc":106,"zd":107}',
                ),
                (
                    '{"id":3,"za":2,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":"1","yc":114,"zd":115}',
                ),
                (
                    '{"id":3,"za":null,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":null,"yb":"1","yc":114,"zd":115}',
                ),
            ]
        ),
    ),
    # 8
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            yb STRING,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $enriched1 = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} any {table_source}db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            $enriched2 = SELECT e.a AS a, e.b AS b, e.c AS c, e.d AS d, e.e AS e, e.f AS f, za, yb, yc, zd, u.c AS c2, u.d AS d2
                from
                    $enriched1 as e
                left join {streamlookup} any {table_source}db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd, (c2 IS NOT DISTINCT FROM c) as eq1, (d2 IS NOT DISTINCT FROM d) as eq2
                from
                    $enriched2 as e
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"yb":"2","yc":100,"zd":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"e":5,"f":6,"za":1,"yb":"2","yc":100,"zd":101,"eq1":true,"eq2":true}',
                ),
                (
                    '{"id":2,"za":7,"yb":"8","yc":106,"zd":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"e":11,"f":12,"za":7,"yb":"8","yc":106,"zd":107,"eq1":true,"eq2":true}',
                ),
                (
                    '{"id":3,"za":2,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":"1","yc":114,"zd":115,"eq1":true,"eq2":true}',
                ),
                (
                    '{"id":3,"za":null,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":null,"yb":"1","yc":114,"zd":115,"eq1":true,"eq2":true}',
                ),
            ]
        ),
    ),
    # 9
    (
        R'''
            PRAGMA ydb.MaxTasksPerStage = "3";
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            a Int32,
                            b STRING,
                            c Int32,
                            d Int32,
                        )
                    )            ;

            $enriched12 = select u.a as a, u.b as b, u.c as c, u.d as d, u.e as e, u.f as f, e.a as za, e.b as yb, e.c as yc, e.d as zd, u2.c as c2, u2.d as d2
                from
                    $input as e
                left join {streamlookup} any {table_source}db as u
                on(e.a = u.a AND e.b = u.b)
                left join {streamlookup} any {table_source}db as u2
                on(e.b = u2.b AND e.a = u2.a)
            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd, (c2 IS NOT DISTINCT FROM c) as eq1, (d2 IS NOT DISTINCT FROM d) as eq2
                from
                    $enriched12 as e
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"a":1,"b":"2","c":100,"d":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"e":5,"f":6,"za":1,"yb":"2","yc":100,"zd":101,"eq1":true,"eq2":true}',
                ),
                (
                    '{"id":2,"a":7,"b":"8","c":106,"d":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"e":11,"f":12,"za":7,"yb":"8","yc":106,"zd":107,"eq1":true,"eq2":true}',
                ),
                (
                    '{"id":3,"a":2,"b":"1","c":114,"d":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":"1","yc":114,"zd":115,"eq1":true,"eq2":true}',
                ),
                (
                    '{"id":3,"a":null,"b":"1","c":114,"d":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":null,"yb":"1","yc":114,"zd":115,"eq1":true,"eq2":true}',
                ),
            ]
        ),
        'ShuffleMode',
        'Map',
    ),
    # 10
    (
        R'''
            PRAGMA ydb.MaxTasksPerStage = "1";

            $input = SELECT * FROM {topic_source}`{input_topic}`;

            $enriched = select
                            e.Data as data, u.id as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(AsList(e.Data) = u.data)
                -- MultiGet true
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            ('ydb10', '{"data":"ydb10","lookup":[1]}'),
            ('ydb20', '{"data":"ydb20","lookup":[2]}'),
            ('ydb30', '{"data":"ydb30","lookup":[3]}'),
            ('ydb40', '{"data":"ydb40","lookup":[null]}'),
            ('ydb50', '{"data":"ydb50","lookup":[null]}'),
            ('ydb10', '{"data":"ydb10","lookup":[1]}'),
            ('ydb20', '{"data":"ydb20","lookup":[2]}'),
            ('ydb30', '{"data":"ydb30","lookup":[3]}'),
            ('ydb40', '{"data":"ydb40","lookup":[null]}'),
            ('ydb50', '{"data":"ydb50","lookup":[null]}'),
        ]
        * 10,
        'MultiGet',
        'true',
        'ShuffleMode',
        'Map',
    ),
    # 11
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            user List<Int32?>?,
                            user_is_null Bool?
                        )
                    )            ;
            $input = SELECT id, case when user_is_null then NULL else user end as user from $input;

            $enriched = select e.id as id,
                            e.user as user_id,
                            u.data as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(e.user = u.id)
                -- MultiGet true
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                ('{"id":3,"user":[5]}', '{"id":3,"user_id":[5],"lookup":[null]}'),
                ('{"id":9,"user":[3]}', '{"id":9,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":2,"user":[2]}', '{"id":2,"user_id":[2],"lookup":["ydb20"]}'),
                (
                    json.dumps(
                        {
                            "id": 111,
                            "user": (L := [*map(lambda _: random.randint(0, 2000), range(random.randint(2, 5000)))]),
                        }
                    ),
                    json.dumps(
                        {
                            "id": 111,
                            "user_id": L,
                            "lookup": [*map(lambda x: f"ydb{x}0" if 1 <= x <= 3 else None, L)],
                        }
                    ),
                ),
                ('{"id":1,"user":[1]}', '{"id":1,"user_id":[1],"lookup":["ydb10"]}'),
                (
                    '{"id":3,"user":[5,3,2,1,0]}',
                    '{"id":3,"user_id":[5,3,2,1,0],"lookup":[null,"ydb30","ydb20","ydb10",null]}',
                ),
                ('{"id":9,"user":[3]}', '{"id":9,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":2,"user":[2]}', '{"id":2,"user_id":[2],"lookup":["ydb20"]}'),
                ('{"id":1,"user":[1]}', '{"id":1,"user_id":[1],"lookup":["ydb10"]}'),
                ('{"id":10,"user":[null]}', '{"id":10,"user_id":[null],"lookup":[null]}'),
                ('{"id":4,"user":[3]}', '{"id":4,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":5,"user":[3]}', '{"id":5,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":6,"user":[1]}', '{"id":6,"user_id":[1],"lookup":["ydb10"]}'),
                ('{"id":7,"user":[2]}', '{"id":7,"user_id":[2],"lookup":["ydb20"]}'),
                ('{"id":10,"user":[]}', '{"id":10,"user_id":[],"lookup":[]}'),
                # ('{"id":10}', '{"id":10,"user_id":null,"lookup":null}'), -- does not work as expected, "user" is parsed as [] instead of NULL
                # ('{"id":10,"user":null}', '{"id":10,"user_id":null,"lookup":null}'), -- does not work as expected either, "user" is parsed as [] instead of NULL
                ('{"id":10,"user_is_null":true}', '{"id":10,"user_id":null,"lookup":null}'),
            ]
            * 20
        ),
        "MultiGet",
        "true",
        "TTL",
        str(random.randint(1, 10)),
        "MaxCachedRows",
        str(random.randint(7, 180)),
        "MaxDelayedRows",
        str(random.randint(1, 1000)),
    ),
    # 12
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            user List<Int32>
                        )
                    )            ;

            $enriched = select e.id as id,
                            e.user as user_id,
                            u.data as lookup
                from
                    $input as e
                left join {streamlookup} any {table_source}{table_name} as u
                on(e.user = u.id)
                -- MultiGet true
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                ('{"id":3,"user":[5]}', '{"id":3,"user_id":[5],"lookup":[null]}'),
                ('{"id":9,"user":[3]}', '{"id":9,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":2,"user":[2]}', '{"id":2,"user_id":[2],"lookup":["ydb20"]}'),
                ('{"id":1,"user":[1]}', '{"id":1,"user_id":[1],"lookup":["ydb10"]}'),
                (
                    '{"id":3,"user":[5,3,2,1,0]}',
                    '{"id":3,"user_id":[5,3,2,1,0],"lookup":[null,"ydb30","ydb20","ydb10",null]}',
                ),
                ('{"id":9,"user":[3]}', '{"id":9,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":2,"user":[2]}', '{"id":2,"user_id":[2],"lookup":["ydb20"]}'),
                ('{"id":1,"user":[1]}', '{"id":1,"user_id":[1],"lookup":["ydb10"]}'),
                ('{"id":4,"user":[3]}', '{"id":4,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":5,"user":[3]}', '{"id":5,"user_id":[3],"lookup":["ydb30"]}'),
                ('{"id":6,"user":[1]}', '{"id":6,"user_id":[1],"lookup":["ydb10"]}'),
                ('{"id":7,"user":[2]}', '{"id":7,"user_id":[2],"lookup":["ydb20"]}'),
                ('{"id":10,"user":[]}', '{"id":10,"user_id":[],"lookup":[]}'),
            ]
            * 20
        ),
        "MultiGet",
        "true",
        "TTL",
        "10",
        "MaxCachedRows",
        "7",
        "MaxDelayedRows",
        "100",
    ),
    # 13
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za List<Int32?>,
                            yb List<STRING>,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $listified = SELECT * FROM {table_source}db;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} any $listified as u
                on(e.za = u.a AND e.yb = u.b)
                -- MultiGet true
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":[1,7],"yb":["2","8"],"yc":100,"zd":101}',
                    '{"a":[1,7],"b":["2","8"],"c":[3,9],"d":[4,10],"e":[5,11],"f":[6,12],"za":[1,7],"yb":["2","8"],"yc":100,"zd":101}',
                ),
                (
                    '{"id":2,"za":[7,13],"yb":["8"],"yc":106,"zd":107}',
                    '{"a":[7],"b":["8"],"c":[9],"d":[10],"e":[11],"f":[12],"za":[7,13],"yb":["8"],"yc":106,"zd":107}',
                ),
                (
                    '{"id":3,"za":[2,null],"yb":["1","1"],"yc":114,"zd":115}',
                    '{"a":[null,null],"b":[null,null],"c":[null,null],"d":[null,null],"e":[null,null],"f":[null,null],"za":[2,null],"yb":["1","1"],"yc":114,"zd":115}',
                ),
            ]
        ),
        "MultiGet",
        "true",
        "MaxCachedRows",
        "0",
    ),
    # 14
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            lza List<Int32?>,
                            lyb List<STRING>,
                            sza Int32?,
                            syb STRING,
                            yc Int32,
                        )
                    )            ;

            $listified = SELECT * FROM {table_source}db;

            $enriched = select u.a as la, u.b as lb, u.c as lc, u2.a as sa, u2.b as sb, u2.c as sc, lza, lyb, sza, syb, yc
                from
                    $input as e
                left join {streamlookup} any $listified as u
                on(e.lza = u.a AND e.lyb = u.b)
                left join /*+streamlookup()*/ any $listified as u2
                on(e.sza = u2.a AND e.syb = u2.b)
                -- MultiGet true
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"lza":[1,7],"lyb":["2","8"],"sza":7,"syb":"8","yc":100}',
                    '{"la":[1,7],"lb":["2","8"],"lc":[3,9],"lza":[1,7],"lyb":["2","8"],"sa":7,"sb":"8","sc":9,"sza":7,"syb":"8","yc":100}',
                ),
                (
                    '{"id":3,"lza":[2,null],"lyb":["1","1"],"sza":2,"syb":"1","yc":114}',
                    '{"la":[null,null],"lb":[null,null],"lc":[null,null],"lza":[2,null],"lyb":["1","1"],"yc":114,"sza":2,"syb":"1","sa":null,"sb":null,"sc":null}',
                ),
            ]
        ),
        "MultiGet",
        "true",
    ),
    # 15
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $formatTime = DateTime::Format("%H:%M:%S");

            $enriched = select e.id as id,
                            $formatTime(DateTime::ParseIso8601(e.ts)) as ts,
                            e.user as user_id,
                            u.id as uid,
                            u.name as name,
                            u.age as age
                from
                    $input as e
                left join {streamlookup} {table_source}`users` as u
                on(e.user = u.age)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"ts":"20240701T113344","ev_type":"foo1","user":25}',
                    '{"id":1,"ts":"11:33:44","uid":2,"user_id":25,"name":"Petr","age":25}',
                ),
                (
                    '{"id":2,"ts":"20240701T112233","ev_type":"foo2","user":15}',
                    '{"id":2,"ts":"11:22:33","uid":1,"user_id":15,"name":"Anya","age":15}',
                    '{"id":2,"ts":"11:22:33","uid":5,"user_id":15,"name":"Irina","age":15}',
                ),
                (
                    '{"id":3,"ts":"20240701T012233","ev_type":"foo2","user":15}',
                    '{"id":3,"ts":"01:22:33","uid":1,"user_id":15,"name":"Anya","age":15}',
                    '{"id":3,"ts":"01:22:33","uid":5,"user_id":15,"name":"Irina","age":15}',
                ),
                (
                    '{"id":4,"ts":"20240701T113355","ev_type":"foo3","user":100}',
                    '{"id":4,"ts":"11:33:55","uid":null,"user_id":100,"name":null,"age":null}',
                ),
                (
                    '{"id":5,"ts":"20240701T113356","ev_type":"foo4","user":17}',
                    '{"id":5,"ts":"11:33:56","uid":3,"user_id":17,"name":"Masha","age":17}',
                ),
                (
                    '{"id":6,"ts":"20240701T133357","ev_type":"foo5","user":17}',
                    '{"id":6,"ts":"13:33:57","uid":3,"user_id":17,"name":"Masha","age":17}',
                ),
                (
                    '{"id":7,"ts":"20240701T153357","ev_type":"foo6","user":13}',
                    '{"id":7,"ts":"15:33:57","uid":6,"user_id":13,"name":"Inna","age":13}',
                ),
                (
                    '{"id":8,"ts":"20240701T193355","ev_type":"foo8","user":99}',
                    '{"id":8,"ts":"19:33:55","uid":null,"user_id":99,"name":null,"age":null}',
                ),
                (
                    '{"id":9,"ts":"20240701T203355","ev_type":"foo9","user":98}',
                    '{"id":9,"ts":"20:33:55","uid":null,"user_id":98,"name":null,"age":null}',
                ),
            ]
            * 100
        ),
        "TTL",
        "10",
        "MaxCachedRows",
        "5",
        "MaxDelayedRows",
        "100",
        "FullscanLimit",
        "0",
    ),
    # 16
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            zd Int32
                        )
                    )            ;

            $enriched = select a, b, c, d, f, za, zd,
                               is_odd, is_true, is_false,
                               opt_odd, opt_true, opt_false, opt_null,
                               CAST(ts AS String) AS tss, CAST(tsd AS String) AS tsds
                               /*, CAST(dur AS String) AS durs -- NOT supported by fq_connector */
                from
                    $input as e
                left join {streamlookup} any {table_source}db as u
                on(e.za = u.a )
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"zd":101}',
                    '''{
                        "a":1,"b":"2","c":3,"d":4,"f":6,"za":1,"zd":101,
                        "is_true":true,"is_false":false,"is_odd":true,
                        "opt_true":true,"opt_false":false,"opt_odd":true,"opt_null":null,
                        "tsds":"1970-01-05","tss":"1970-01-03T10:11:12Z"
                    }''',
                ),
                (
                    '{"id":2,"za":7,"zd":107}',
                    '''{
                        "a":7,"b":"8","c":9,"d":10,"f":12,"za":7,"zd":107,
                        "is_true":true,"is_false":false,"is_odd":true,
                        "opt_true":true,"opt_false":false,"opt_odd":true,"opt_null":null,
                        "tsds":"1970-01-06","tss":"1970-01-03T10:11:13Z"
                    }''',
                ),
                (
                    '{"id":3,"za":33,"zd":133}',
                    '''{
                        "a":null,"b":null,"c":null,"d":null,"f":null,"za":33,"zd":133,
                        "is_true":null,"is_false":null,"is_odd":null,
                        "opt_true":null,"opt_false":null,"opt_odd":null,"opt_null":null,
                        "tsds":null,"tss":null
                    }''',
                ),
                (
                    '{"id":2,"za":2,"zd":102}',
                    '''{
                        "a":2,"b":"3","c":6,"d":null,"f":9,"za":2,"zd":102,
                        "is_true":true,"is_false":false,"is_odd":false,
                        "opt_true":true,"opt_false":false,"opt_odd":false,"opt_null":null,
                        "tsds":"1970-01-07","tss":"1970-01-03T10:11:14Z"
                    }''',
                ),
                (
                    '{"id":4,"za":4,"zd":104}',
                    '''{
                        "a":4,"b":"5","c":4,"d":15,"f":null,"za":4,"zd":104,
                        "is_true":true,"is_false":false,"is_odd":false,
                        "opt_true":true,"opt_false":false,"opt_odd":false,"opt_null":null,
                        "tsds":"1970-01-08","tss":"1970-01-03T10:11:15Z"
                    }''',
                ),
            ]
            * 1000
        ),
        "TTL",
        "1",
    ),
    # 17
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            zd Int32
                        )
                    )            ;

            $enriched = select a, b, c, d, f, za, zd,
                               is_odd, is_true, is_false,
                               opt_odd, opt_true, opt_false, opt_null
                from
                    $input as e
                left join {streamlookup} any {table_source}db as u
                on(e.za = u.a )
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"zd":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"f":6,"za":1,"zd":101,"is_true":true,"is_false":false,"is_odd":true,"opt_true":true,"opt_false":false,"opt_odd":true,"opt_null":null}',
                ),
                (
                    '{"id":2,"za":7,"zd":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"f":12,"za":7,"zd":107,"is_true":true,"is_false":false,"is_odd":true,"opt_true":true,"opt_false":false,"opt_odd":true,"opt_null":null}',
                ),
                (
                    '{"id":3,"za":33,"zd":133}',
                    '{"a":null,"b":null,"c":null,"d":null,"f":null,"za":33,"zd":133,"is_true":null,"is_false":null,"is_odd":null,"opt_true":null,"opt_false":null,"opt_odd":null,"opt_null":null}',
                ),
                (
                    '{"id":2,"za":2,"zd":102}',
                    '{"a":2,"b":"3","c":6,"d":null,"f":9,"za":2,"zd":102,"is_true":true,"is_false":false,"is_odd":false,"opt_true":true,"opt_false":false,"opt_odd":false,"opt_null":null}',
                ),
                (
                    '{"id":4,"za":4,"zd":104}',
                    '{"a":4,"b":"5","c":4,"d":15,"f":null,"za":4,"zd":104,"is_true":true,"is_false":false,"is_odd":false,"opt_true":true,"opt_false":false,"opt_odd":false,"opt_null":null}',
                ),
            ]
            * 1000
        ),
        "TTL",
        "1",
        "MaxCachedRows",
        "4",
    ),
    # 18
    (
        R'''
            $input = SELECT * FROM {topic_source}`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $formatTime = DateTime::Format("%H:%M:%S");

            $enriched = select e.id as id,
                            $formatTime(DateTime::ParseIso8601(e.ts)) as ts,
                            e.user as user_id,
                            u.id as uid,
                            u.name as name,
                            u.age as age
                from
                    $input as e
                left join {streamlookup} any {table_source}`users` as u
                on(e.user = u.id)
            ;

            $enriched = select e.id as id,
                            e.ts as ts,
                            e.user_id as user_id,
                            u2.id as uid,
                            u2.name as name,
                            u2.age as age
                from
                    $enriched as e
                left join {streamlookup} any {table_source}`users` as u2
                on(e.name = u2.name and u2.age = e.age)
            ;

            insert into {topic_source}`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                    '{"id":1,"ts":"11:33:44","uid":2,"user_id":2,"name":"Petr","age":25}',
                ),
                (
                    '{"id":2,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                    '{"id":2,"ts":"11:22:33","uid":1,"user_id":1,"name":"Anya","age":15}',
                ),
                (
                    '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":100}',
                    '{"id":3,"ts":"11:33:55","uid":null,"user_id":100,"name":null,"age":null}',
                ),
                (
                    '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                    '{"id":4,"ts":"11:33:56","uid":3,"user_id":3,"name":"Masha","age":17}',
                ),
                (
                    '{"id":5,"ts":"20240701T113357","ev_type":"foo5","user":3}',
                    '{"id":5,"ts":"11:33:57","uid":3,"user_id":3,"name":"Masha","age":17}',
                ),
                (
                    '{"id":6,"ts":"20240701T112238","ev_type":"foo6","user":1}',
                    '{"id":6,"ts":"11:22:38","uid":1,"user_id":1,"name":"Anya","age":15}',
                ),
                (
                    '{"id":7,"ts":"20240701T113349","ev_type":"foo7","user":2}',
                    '{"id":7,"ts":"11:33:49","uid":2,"user_id":2,"name":"Petr","age":25}',
                ),
            ]
            * 1000
        ),
        "TTL",
        "10",
        "MaxCachedRows",
        "5",
        "MaxDelayedRows",
        "100",
        "ShuffleMode",
        "Hash",
        "FullscanLimit",
        "0",
    ),
]


class TestJoinYdbStreaming(StreamingTestBase):
    @pytest.mark.parametrize("partitions_count", [1, 3] if DEBUG else [3])
    @pytest.mark.parametrize("streamlookup", [True, False], ids=["slj", "map"])
    @pytest.mark.parametrize("testcase", [*range(len(TESTCASES))])
    @pytest.mark.parametrize("local", [True, False], ids=["local", "generic"])
    @pytest.mark.parametrize("column_tables", [True, False], ids=["cs", "row"])
    def test_streamlookup(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        testcase: int,
        streamlookup: bool,
        partitions_count: int,
        local: bool,
        column_tables: bool,
    ):
        if not (DEBUG or streamlookup):
            pytest.skip("map join verified only in DEBUG test")
        if local and streamlookup:
            pytest.skip("YQ-5431")
        title = f"slj_{partitions_count}{str(streamlookup)[:1]}{testcase}"
        query_name = f"q_{title}"
        endpoint = self.get_endpoint(kikimr, local_topics=True)
        source_name = entity_name("join_source")
        self.init_topics(source_name, create_output=True, partitions_count=partitions_count, endpoint=endpoint)

        # 1. Create the secret
        secret_name = entity_name("token_secret")
        create_secret(kikimr, secret_name)

        # 2. Create and populate local table
        create_table(kikimr, column_tables)

        # 3. Create TOKEN-auth external data source.
        if not local:
            create_source(kikimr, source_name, secret_name, endpoint)

        table_name = 'join_table'

        sql, messages, *options = TESTCASES[testcase]
        sql = sql.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
            table_name=table_name,
            table_source='' if local else f"{source_name}.",
            topic_source='',
            streamlookup=Rf'/*+ streamlookup({" ".join(options)}) */' if streamlookup else '',
        )

        # options_dict = dict(zip(islice(options, 0, None, 2), islice(options, 1, None, 2)))

        try:
            self.create_streaming_query(kikimr, query_name, f"""
                CREATE STREAMING QUERY {query_name} AS DO BEGIN
                {sql}
                END DO;
            """)
        except ydb.issues.Error as ex:
            assert not streamlookup and "MultiGet true" in sql, ex
            assert "Cannot compare key columns" in ex.message
            return

        assert not (not streamlookup and "MultiGet true" in sql)
        path = f"/Root/{query_name}"
        self.wait_completed_checkpoints(kikimr, path)

        for offset in range(0, len(messages), MAX_WRITE_STREAM_SIZE):
            self.write_stream(map(lambda x: x[0], messages[offset : offset + MAX_WRITE_STREAM_SIZE]), endpoint=endpoint)

        expected_len = sum(map(len, messages)) - len(messages)
        read_data = self.read_stream(expected_len, topic_path=self.output_topic, endpoint=endpoint)
        read_data_ctr = Counter(map(freeze, map(json.loads, read_data)))
        messages_ctr = Counter(map(freeze, map(json.loads, chain(*map(lambda row: islice(row, 1, None), messages)))))
        assert read_data_ctr == messages_ctr

        """ TODO dq_tasks sensors unavailable in ydb streaming
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            sensors = kikimr.compute_plane.get_sensors(node_index, "dq_tasks")
            for component in ["Lookup", "LookupSrc"]:
                componentSensors = sensors.find_sensors(
                    labels={"operation": query_id, "component": component},
                    key_label="sensor",
                )
                if component == "LookupSrc":
                    if options_dict.get("FullscanLimit") == "0" or (
                        "FullscanLimit" not in options_dict and options_dict.get("MaxCachedRows") == "0"
                    ):
                        assert componentSensors.get("Fullscans", 0) == 0
                for k in componentSensors:
                    print(
                        f'node[{node_index}].operation[{query_id}].component[{component}].{k} = {componentSensors[k]}',
                        file=sys.stderr,
                    )
        """

        kikimr.ydb_client.query(f"DROP STREAMING QUERY {query_name}")
        if not local:
            kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE {source_name}")

    @pytest.mark.parametrize("partitions_count", [1, 2])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("streamlookup", [True, False], ids=["slj", "map"])
    @pytest.mark.parametrize("local", [True, False], ids=["local", "generic"])
    @pytest.mark.parametrize("column_tables", [False])
    def test_streamlookup_watermarks(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        streamlookup,
        tasks,
        partitions_count,
        local: bool,
        column_tables: bool,
    ):
        if local and streamlookup:
            pytest.skip("YQ-5431")
        pytest.skip("YQ-5580: works unstable, requires investigation")
        title = f"slj_wm_{partitions_count}{streamlookup!s:.1}{tasks}{local!s:.1}"
        query_name = f"q_{title}"
        endpoint = self.get_endpoint(kikimr, local_topics=True)
        source_name = entity_name("join_wm_source")
        self.init_topics(source_name, create_output=True, partitions_count=partitions_count, endpoint=endpoint)

        # 1. Create the secret
        secret_name = entity_name("token_secret")
        create_secret(kikimr, secret_name)

        # 2. Create and populate local table
        create_table(kikimr, column_tables)

        table_name = 'join_table'

        # 3. Create TOKEN-auth external data source.
        # (shared_reading with local topics is not implemented, hence use eds)
        create_source(kikimr, source_name, secret_name, endpoint, shared_reading=True)

        options = ()
        streamlookup_hint = Rf'/*+ streamlookup({" ".join(options)}) */' if streamlookup else ''
        idle_clause = R", WATERMARK_IDLE_TIMEOUT = 'PT5S'" if tasks > 1 or partitions_count > 1 else ""
        table_source = '' if local else f"`{source_name}`."
        topic_source = f'`{source_name}`.'
        sql = Rf'''
            PRAGMA ydb.MaxTasksPerStage = "{tasks}";

            $event_time = ($ts) -> (CAST(($ts*1000000ul) AS Timestamp));

            $input = SELECT * FROM {topic_source}`{self.input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            ts Uint64,
                            user Int32,
                            skip Bool
                        )
                        , WATERMARK = $event_time(`ts`) - Interval('PT3S')
                        , WATERMARK_GRANULARITY = 'PT2S'
                        {idle_clause}
                    )            ;
            $input =
                SELECT e.*, $event_time(ts) AS event_time FROM $input AS e WHERE skip IS DISTINCT FROM true;
            $enriched = SELECT event_time, ts, u.data as uid
                FROM
                    $input as e
                LEFT JOIN {streamlookup_hint} ANY {table_source}{table_name} AS u
                ON(e.user = u.id)
            ;
            $enriched =
                SELECT CAST(HOP_END() AS Uint64)/1000000ul as hopTime, uid, ListSort(AGGREGATE_LIST(ts)) AS tsList FROM $enriched
                    GROUP BY HoppingWindow(event_time, 'PT5S', 'PT10S', "max" AS TimeLimit)
                            , uid
                ;

            insert into {topic_source}`{self.output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            '''

        messages = [
            (R'{"ts":12, "user": 1}',),  # ############ 0 # w 9->8
            (R'{"ts":10, "user": 1}',),  # ############ 1 # w 7->6
            (R'{"ts":11, "user": 2}',),  # ############ 2 # w 8->8
            (R'{"ts":13, "user": 10}',),  # ########### 3 # w 10->10 -> close :=10
            (R'{"ts":16, "user": 3}',),  # ############ 4 # w 13->12
            ('{"ts":17, "user": 1, "skip": true}',),  # 5 # w 14->14
            (
                R'{"ts":19, "user": 4}',  # ########### 6 # w 16->16 -> close :=15
                R'{"uid": null,   "hopTime":15, "tsList":[13]}',
                R'{"uid":"ydb10", "hopTime":15, "tsList":[10, 12]}',
                R'{"uid":"ydb20", "hopTime":15, "tsList":[11]}',
            ),
            (R'{"ts":18, "user": 4}',),  # ############ 7 # w 15->14
            (R'{"ts":21, "user": 9}',),  # ############ 8 # w 18->18
            (  # ###################################### 9 # w 25 -> 24 -> close :=20
                R'{"ts":28, "user": 5, "skip": true}',
                R'{"uid": null,   "hopTime":20, "tsList":[13, 18, 19]}',
                R'{"uid":"ydb10", "hopTime":20, "tsList":[10, 12]}',
                R'{"uid":"ydb20", "hopTime":20, "tsList":[11]}',
                R'{"uid":"ydb30", "hopTime":20, "tsList":[16]}',
            ),
        ]
        self.create_streaming_query(kikimr, query_name, f"""
            CREATE STREAMING QUERY {query_name} AS DO BEGIN
            {sql}
            END DO;
        """)

        path = f"/Root/{query_name}"
        self.wait_completed_checkpoints(kikimr, path)

        if partitions_count > 1 or tasks > 1:
            # let idle timeout fire
            time.sleep(5.0)

        for pair in messages:
            self.write_stream(
                [pair[0]],
                partition_key=b'1',
                endpoint=endpoint,
            )
            expected = pair[1:]
            read_data = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint, timeout=None if len(expected) > 0 else 10)
            read_data_ctr = Counter(map(freeze, map(json.loads, read_data)))
            messages_ctr = Counter(map(freeze, map(json.loads, expected)))
            assert read_data_ctr == messages_ctr

        """ TODO dq_tasks sensors unavailable in ydb streaming
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            sensors = kikimr.compute_plane.get_sensors(node_index, "dq_tasks")
            for component in ["Lookup", "LookupSrc"]:
                componentSensors = sensors.find_sensors(
                    labels={"operation": query_id, "component": component},
                    key_label="sensor",
                )
                for k in componentSensors:
                    print(
                        f'node[{node_index}].operation[{query_id}].component[{component}].{k} = {componentSensors[k]}',
                        file=sys.stderr,
                    )
            sensors = kikimr.compute_plane.get_sensors(node_index, "yq")
            mkqlSensors = sensors.find_sensors(
                labels={"query_id": query_id, "sensor": "MkqlMaxMemoryUsage"},
                key_label="Stage",
            )
            for k in mkqlSensors:
                print(
                    f'node[{node_index}].query_id[{query_id}].Stage[{k}].MkqlMaxMemoryUsage = {mkqlSensors[k]}',
                    file=sys.stderr,
                )
        """
        kikimr.ydb_client.query(f"DROP STREAMING QUERY {query_name}")
        kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE {source_name}")
