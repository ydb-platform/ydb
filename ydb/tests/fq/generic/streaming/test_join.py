import pytest
import os
import json
import sys
import time
from collections import Counter
from itertools import chain, islice

import operator
import ydb

from ydb.tests.fq.generic.utils.ydb_helpers import create_external_data_source
from ydb.tests.fq.streaming_common.common import StreamingTestBase, counter_nodes, get_sensors
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule

from ydb.library.yql.providers.generic.connector.tests.utils.one_time_waiter import OneTimeWaiter
from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind

import conftest
import random

MAX_WRITE_STREAM_SIZE = 500
DEBUG = 0
SEED = 0  # use fixed seed for regular tests
if DEBUG:
    if "RANDOM_SEED" in os.environ:
        SEED = int(os.environ["RANDOM_SEED"])
    else:
        SEED = random.randint(0, (1 << 31))
        print(f"RANDOM_SEED={SEED}", file=sys.stderr)
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
    # Designed for (deserialized) obj
    t = type(obj)
    if t is dict:
        return frozenset((k, freeze(v)) for k, v in obj.items())
    if t is list:
        return tuple(map(freeze, obj))
    return obj


TESTCASES = [
    # 0
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select
                            e.Data as data, u.id as lookup
                from
                    $input as e
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(e.Data = u.data)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select
                            e.Data as data, CAST(e.Data AS Int32) as id, u.data as lookup
                from
                    $input as e
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(CAST(e.Data AS Int32) = u.id)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(e.user = u.id)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(e.user = u.id)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.`users` as u
                on(e.user = u.id)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.`users` as eu
                on(e.user = eu.id)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.yb = u.b AND e.za = u.a )
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            $enriched2 = SELECT e.a AS a, e.b AS b, e.c AS c, e.d AS d, e.e AS e, e.f AS f, za, yb, yc, zd, u.c AS c2, u.d AS d2
                from
                    $enriched1 as e
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd, (c2 IS NOT DISTINCT FROM c) as eq1, (d2 IS NOT DISTINCT FROM d) as eq2
                from
                    $enriched2 as e
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.a = u.a AND e.b = u.b)
                left join {streamlookup} any ydb_conn_{table_name}.db as u2
                on(e.b = u2.b AND e.a = u2.a)
            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd, (c2 IS NOT DISTINCT FROM c) as eq1, (d2 IS NOT DISTINCT FROM d) as eq2
                from
                    $enriched12 as e
            ;

            insert into myyds.`{output_topic}`
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

            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select
                            e.Data as data, u.id as lookup
                from
                    $input as e
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(AsList(e.Data) = u.data)
                -- MultiGet true
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(e.user = u.id)
                -- MultiGet true
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.{table_name} as u
                on(e.user = u.id)
                -- MultiGet true
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za List<Int32?>,
                            yb List<STRING>,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $listified = SELECT * FROM ydb_conn_{table_name}.db;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} any $listified as u
                on(e.za = u.a AND e.yb = u.b)
                -- MultiGet true
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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

            $listified = SELECT * FROM ydb_conn_{table_name}.db;

            $enriched = select u.a as la, u.b as lb, u.c as lc, u2.a as sa, u2.b as sb, u2.c as sc, lza, lyb, sza, syb, yc
                from
                    $input as e
                left join {streamlookup} any $listified as u
                on(e.lza = u.a AND e.lyb = u.b)
                left join /*+streamlookup()*/ any $listified as u2
                on(e.sza = u2.a AND e.syb = u2.b)
                -- MultiGet true
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} ydb_conn_{table_name}.`users` as u
                on(e.user = u.age)
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.za = u.a )
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.db as u
                on(e.za = u.a )
            ;

            insert into myyds.`{output_topic}`
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
            $input = SELECT * FROM myyds.`{input_topic}`
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
                left join {streamlookup} any ydb_conn_{table_name}.`users` as u
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
                left join {streamlookup} any ydb_conn_{table_name}.`users` as u2
                on(e.name = u2.name and u2.age = e.age)
            ;

            insert into myyds.`{output_topic}`
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


one_time_waiter = OneTimeWaiter(
    data_source_kind=EGenericDataSourceKind.YDB,
    docker_compose_file_path=conftest.docker_compose_file_path,
    expected_tables=["simple_table", "join_table", "dummy_table"],
)


class TestJoinStreaming(StreamingTestBase):
    def _prepare(self, kikimr, title, partitions_count=1, shared=False):
        endpoint = self.get_endpoint(kikimr, local_topics=False)
        self.init_topics(title, partitions_count=partitions_count, endpoint=endpoint)
        create_read_rule(self.input_topic, self.consumer_name, default_endpoint=endpoint)
        self.create_source(kikimr, "wmyds" if shared else "myyds", shared=shared, endpoint=endpoint)
        create_external_data_source(
            kikimr.ydb_client, "ydb_conn_join_table", "Ydb", "tests-fq-generic-streaming-ydb:2136", "local"
        )
        one_time_waiter.wait()
        return endpoint

    def _start(self, kikimr, query_name, sql):
        kikimr.ydb_client.query(f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                PRAGMA pq.Consumer = "{self.consumer_name}";
                {sql}
            END DO;
        ''')

    def _stop(self, kikimr, query_name):
        kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);")
        path = f"{kikimr.get_database_name()}/{query_name}"
        assert wait_for(
            lambda: [
                row.Status
                for row in kikimr.ydb_client.query(
                    f'SELECT Status FROM `.sys/streaming_queries` WHERE Path = "{path}";'
                )[0].rows
            ]
            == ["STOPPED"],
            timeout_seconds=120,
        ), f"Streaming query did not stop: {path}"

    def _lookup_sensors(self, kikimr, query_name):
        path = f"{kikimr.get_database_name()}/{query_name}"
        for node_index in counter_nodes(kikimr.cluster):
            sensors = get_sensors(kikimr.cluster, node_index, "kqp")
            for component in ("Lookup", "LookupSrc"):
                values = sensors.find_sensors(
                    labels={"subsystem": "DqLookup", "tx_id": path, "component": component},
                    key_label="sensor",
                    combine=operator.add,
                )
                for name, value in values.items():
                    print(f"node[{node_index}].tx_id[{path}].component[{component}].{name} = {value}", file=sys.stderr)
                yield component, values

    def test_simple(self, kikimr):
        title = "pq_ydb_streaming_test_simple"
        endpoint = self._prepare(kikimr, title)
        table_name = "join_table"

        sql = R'''
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select e.Data as Data
                from
                    $input as e
                left join
                    ydb_conn_{table_name}.{table_name} as u
                on(e.Data = CAST(u.id as String))
            ;

            insert into myyds.`{output_topic}`
            select * from $enriched;
            '''.format(input_topic=self.input_topic, output_topic=self.output_topic, table_name=table_name)

        query_name = f"q_{title}"
        self._start(kikimr, query_name, sql)
        self.wait_completed_checkpoints(kikimr, query_name)

        messages = ['A', 'B', 'C']
        self.write_stream(messages, endpoint=endpoint)

        read_data = self.read_stream(len(messages), endpoint=endpoint)
        assert read_data == messages

        self._stop(kikimr, query_name)

    @pytest.mark.parametrize("partitions_count", [1, 3] if DEBUG else [3])
    @pytest.mark.parametrize("streamlookup", [False, True] if DEBUG else [True])
    @pytest.mark.parametrize("testcase", [*range(len(TESTCASES))])
    def test_streamlookup(
        self,
        kikimr,
        testcase,
        streamlookup,
        partitions_count,
    ):
        title = f"slj_{partitions_count}{str(streamlookup)[:1]}{testcase}"
        endpoint = self._prepare(kikimr, title, partitions_count)
        table_name = "join_table"

        sql, messages, *options = TESTCASES[testcase]
        sql = sql.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
            table_name=table_name,
            streamlookup=Rf'/*+ streamlookup({" ".join(options)}) */' if streamlookup else '',
        )

        options_dict = dict(zip(islice(options, 0, None, 2), islice(options, 1, None, 2)))

        query_name = f"q_{title}"
        try:
            self._start(kikimr, query_name, sql)
        except ydb.issues.Error as error:
            assert not streamlookup and "MultiGet true" in sql, error
            assert "Cannot compare key columns" in str(error)
            return
        assert not (not streamlookup and "MultiGet true" in sql)
        self.wait_completed_checkpoints(kikimr, query_name)

        for offset in range(0, len(messages), MAX_WRITE_STREAM_SIZE):
            self.write_stream(map(lambda x: x[0], messages[offset : offset + MAX_WRITE_STREAM_SIZE]), endpoint=endpoint)

        expected_len = sum(map(len, messages)) - len(messages)
        read_data = self.read_stream(expected_len, endpoint=endpoint)
        if DEBUG:
            print(streamlookup, testcase, file=sys.stderr)
            print(sql, file=sys.stderr)
            print(*zip(messages, read_data), file=sys.stderr, sep="\n")
        read_data_ctr = Counter(map(freeze, map(json.loads, read_data)))
        messages_ctr = Counter(map(freeze, map(json.loads, chain(*map(lambda row: islice(row, 1, None), messages)))))
        assert read_data_ctr == messages_ctr

        for component, component_sensors in self._lookup_sensors(kikimr, query_name):
            if component == "LookupSrc":
                if options_dict.get("FullscanLimit") == "0" or (
                    "FullscanLimit" not in options_dict and options_dict.get("MaxCachedRows") == "0"
                ):
                    assert component_sensors.get("Fullscans", 0) == 0

        self._stop(kikimr, query_name)

    @pytest.mark.parametrize("partitions_count", [1, 2])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("streamlookup", [True, False])
    @pytest.mark.parametrize("limit", [6, 7, 8, 9, None])
    def test_streamlookup_watermarks(
        self,
        kikimr,
        limit,
        streamlookup,
        tasks,
        partitions_count,
    ):
        title = f"slj_wm_{partitions_count}{str(streamlookup)[:1]}{limit}{tasks}"
        endpoint = self._prepare(kikimr, title, partitions_count, shared=True)
        table_name = "join_table"

        options = ()
        streamlookup_hint = Rf'/*+ streamlookup({" ".join(options)}) */' if streamlookup else ''
        idle_clause = R", WATERMARK_IDLE_TIMEOUT = 'PT5S'" if tasks > 1 or partitions_count > 1 else ""
        sql = Rf'''
            PRAGMA ydb.MaxTasksPerStage = "{tasks}";

            $event_time = ($ts) -> (CAST(($ts*1000000ul) AS Timestamp));

            $input = SELECT * FROM wmyds.`{self.input_topic}`
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
                LEFT JOIN {streamlookup_hint} ANY ydb_conn_{table_name}.{table_name} AS u
                ON(e.user = u.id)
            ;
            $enriched =
                SELECT CAST(HOP_END() AS Uint64)/1000000ul as hopTime, uid, ListSort(AGGREGATE_LIST(ts)) AS tsList FROM $enriched
                    GROUP BY HoppingWindow(event_time, 'PT5S', 'PT10S', "max" AS TimeLimit)
                            , uid
                ;

            insert into wmyds.`{self.output_topic}`
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
        messages = messages[:limit]

        query_name = f"q_{title}"
        self._start(kikimr, query_name, sql)
        self.wait_completed_checkpoints(kikimr, query_name)

        for offset in range(0, len(messages), MAX_WRITE_STREAM_SIZE):
            self.write_stream(
                map(lambda x: x[0], messages[offset : offset + MAX_WRITE_STREAM_SIZE]),
                partition_key=b'1',
                endpoint=endpoint,
            )
        if partitions_count > 1 or tasks > 1:
            time.sleep(5.0)

        expected_len = sum(map(len, messages)) - len(messages)
        read_data = self.read_stream(expected_len, endpoint=endpoint)
        if DEBUG:
            print(streamlookup, file=sys.stderr)
            print(sql, file=sys.stderr)
            print(*zip(messages, read_data), file=sys.stderr, sep="\n")
        read_data_ctr = Counter(map(freeze, map(json.loads, read_data)))
        messages_ctr = Counter(map(freeze, map(json.loads, chain(*map(lambda row: islice(row, 1, None), messages)))))
        assert read_data_ctr == messages_ctr

        list(self._lookup_sensors(kikimr, query_name))

        self._stop(kikimr, query_name)
