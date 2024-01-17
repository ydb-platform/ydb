from test_base import TestBase
from ydb.core.fq.libs.http_api_client.http_client import YandexQueryHttpClient
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
import ydb.public.api.protos.draft.fq_pb2 as fq
import library.python.retry as retry
import logging
import os
import re

import yaml
from yaml.loader import SafeLoader

import http.client

http.client.HTTPConnection.debuglevel = 1

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

logging.basicConfig(level=logging.DEBUG)


@retry.retry(retry.RetryConf().upto(10))
def wait_for_query_status(client, query_id, statuses):
    status_json = client.get_query_status(query_id)
    status = status_json["status"]
    if status not in statuses:
        raise Exception(f"Status {status} is not in {statuses}")
    return status


def normalize_timestamp_string(s):
    # 1970-01-01T00:00:00Z - > zero_time
    # s = s.replace("1970-01-01T00:00:00Z", "zero_time")

    # 2022-08-13T16:11:21Z -> ISOTIME
    # 2022-08-13T16:11:21.549879Z -> ISOTIME
    return re.sub(r"2\d{3}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z", "ISOTIME", s)


def pack_stable_json(s):
    # "id": "ptqugprog25is2bvsa0u" -> "id": "xxxxxxxxxxxxxxxxxxxx"
    s = re.sub(r'"id": "[\w\d]{20}"', '"id": "xxxxxxxxxxxxxxxxxxxx"', s)
    s = normalize_timestamp_string(s)
    logging.info(s)
    return s


def normalize_property(name, value):
    if name == "id":
        if len(value) == 20:
            return "xxxxxxxxxxxxxxxxxxxx"
        if len(value) == 41:
            return "xxxxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyyyyyy"
        return value
    if isinstance(value, str):
        return normalize_timestamp_string(value)
    return value


def normalize_json(data):
    if isinstance(data, list):
        return [normalize_json(d) for d in data]
    if isinstance(data, dict):
        return {k: normalize_property(k, normalize_json(v)) for k, v in data.items()}
    return data


class TestHttpApi(TestBase):
    def create_client(self, token="root@builtin"):
        return YandexQueryHttpClient(self.streaming_over_kikimr.http_api_endpoint(),
                                     project="my_folder",
                                     token=token)

    def test_simple_analitycs_query(self):
        client = self.create_client()
        result = client.create_query("select 1", name="my first query", description="some description")
        assert type(result) is dict, type(result)
        assert len(result) == 1
        query_id = result.get("id")
        assert query_id is not None
        assert len(query_id) == 20

        status_json = client.get_query_status(query_id)
        assert len(status_json) == 1
        assert status_json["status"] in ["FAILED", "RUNNING", "COMPLETED"]

        wait_for_query_status(client, query_id, ["COMPLETED"])
        query_json = client.get_query(query_id)
        assert normalize_json(query_json) == {
            "id": "xxxxxxxxxxxxxxxxxxxx",
            "name": "my first query",
            "description": "some description",
            "text": "select 1",
            "type": "ANALYTICS",
            "status": "COMPLETED",
            "meta": {
                "finished_at": "ISOTIME",
                "started_at": "ISOTIME"
            },
            "result_sets": [
                {
                    "rows_count": 1,
                    "truncated": False
                }
            ]
        }
        assert query_json["id"] == query_id

        results_json = client.get_query_results(query_id, 0)
        assert normalize_json(results_json) == {
            'columns': [{'name': 'column0', 'type': 'Int32'}],
            'rows': [[1]]
        }

        response = client.stop_query(query_id)
        assert response.status_code == 204

    def test_empty_query(self):
        client = self.create_client()
        try:
            client.create_query()
        except Exception as e:
            assert "\"message\":\"text\'s length is not in [1; 102400]" in e.args[0]
            return
        assert False

    def test_warning(self):
        client = self.create_client()
        result = client.create_query(query_text="select 10000000000000000000+1")
        query_id = result.get("id")

        wait_for_query_status(client, query_id, ["COMPLETED"])
        query_json = client.get_query(query_id)
        # todo: issues should not be empty, should be fixed in YQ-1613
        assert normalize_json(query_json) == {
            "id": "xxxxxxxxxxxxxxxxxxxx",
            "name": "",
            "description": "",
            "text": "select 10000000000000000000+1",
            "type": "ANALYTICS",
            "status": "COMPLETED",
            "issues": {
                "message": "{ <main>: Warning: Type annotation, code: 1030 subissue: { <main>:1:1: Warning: At function: "
                           "RemovePrefixMembers, At function: Unordered, At function: PersistableRepr, At function: "
                           "OrderedSqlProject, At function: SqlProjectItem subissue: { <main>:1:28: Warning: At function: + "
                           "subissue: { <main>:1:28: Warning: Integral type implicit bitcast: Uint64 and Int32, code: 1107 } } } }",
                "details": [
                    {
                        "message": "Type annotation",
                        "severity": "WARNING",
                        "issue_code": 1030,
                        "issues": [
                            {
                                "message": "At function: RemovePrefixMembers, At function: Unordered, At "
                                           "function: PersistableRepr, At function: OrderedSqlProject, At function: SqlProjectItem",
                                "severity": "WARNING",
                                "position": {"column": 1, "row": 1},
                                "end_position": {"column": 1, "row": 1},
                                "issues": [
                                    {
                                        "message": 'At function: +',
                                        "position": {"column": 28, "row": 1},
                                        "severity": "WARNING",
                                        "end_position": {"column": 28, "row": 1},
                                        "issues": [
                                            {
                                                "message": "Integral type implicit bitcast: Uint64 and Int32",
                                                "severity": "WARNING",
                                                "position": {"column": 28, "row": 1},
                                                "end_position": {"column": 28, "row": 1},
                                                "issue_code": 1107,
                                                "issues": []
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            "meta": {
                "finished_at": "ISOTIME",
                "started_at": "ISOTIME"
            },
            "result_sets": [
                {
                    "rows_count": 1,
                    "truncated": False
                }
            ]
        }

    def test_get_unknown_query(self):
        client = self.create_client()

        response = client.get_query("bad_id", expected_code=400)
        assert response == {
            "status": 400010,
            "message": "BAD_REQUEST",
            "details": [
                {
                    "message": "Query does not exist or permission denied. Please check the id of the query or your access rights",
                    "issue_code": 1000,
                    "severity": "ERROR",
                    "issues": []
                }
            ]
        }

    def test_unauthenticated(self):
        client = self.create_client(token="zzz")

        response = client.create_query("select 1", name="my first query", description="some description",
                                       expected_code=403)
        assert response == {
            "status": 400020,
            "message": "UNAUTHORIZED",
            "details": [
                {
                    "issues": [],
                    "message": "Authorization error. Permission denied",
                    "severity": "ERROR"
                }
            ]
        }

    def test_create_idempotency(self):
        self.init_topics("idempotency", create_output=False)
        c = FederatedQueryClient("my_folder", streaming_over_kikimr=self.streaming_over_kikimr)
        c.create_yds_connection("yds2", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        sql = f"select * from yds2.`{self.input_topic}`"

        client = self.create_client()
        result1 = client.create_query(sql, name="my first query", description="some description",
                                      idempotency_key="X")
        result2 = client.create_query("select 2", name="my first query2", description="some description2",
                                      idempotency_key="X")
        result3 = client.create_query("select 3", name="my first query3", description="some description3",
                                      idempotency_key="Y")
        query_id1 = result1["id"]
        query_id2 = result2["id"]
        query_id3 = result3["id"]
        assert query_id1 == query_id2
        assert query_id1 != query_id3

        query1_json = client.get_query(query_id1)
        query1_json["text"] = sql

        response = client.stop_query(query_id1)
        assert response.status_code == 204

    def test_stop_idempotency(self):
        client = self.create_client()
        c = FederatedQueryClient("my_folder", streaming_over_kikimr=self.streaming_over_kikimr)
        self.streaming_over_kikimr.compute_plane.stop()
        query_id = c.create_query("select1", "select 1").result.query_id
        c.wait_query_status(query_id, fq.QueryMeta.STARTING)

        response1 = client.stop_query(query_id, idempotency_key="Z")
        assert response1.status_code == 204
        response2 = client.stop_query(query_id, idempotency_key="Z")
        assert response2.status_code == 204
        client.stop_query(query_id, expected_code=400)

        self.streaming_over_kikimr.compute_plane.start()
        c.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)

    def test_simple_streaming_query(self):
        self.init_topics("simple_streaming_query", create_output=False)
        c = FederatedQueryClient("my_folder", streaming_over_kikimr=self.streaming_over_kikimr)
        c.create_yds_connection("yds1", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        sql = f"select * from yds1.`{self.input_topic}`"

        client = self.create_client()
        result = client.create_query(sql, type="STREAMING", name="my first query",
                                     description="some description")
        assert type(result) is dict, type(result)
        assert len(result) == 1
        query_id = result.get("id")
        assert query_id is not None
        assert len(query_id) == 20

        status_json = client.get_query_status(query_id)
        assert len(status_json) == 1
        assert status_json["status"] in ["FAILED", "RUNNING", "COMPLETED"]

        wait_for_query_status(client, query_id, ["RUNNING"])
        query_json = client.get_query(query_id)
        assert normalize_json(query_json) == {
            "id": "xxxxxxxxxxxxxxxxxxxx",
            "name": "my first query",
            "description": "some description",
            "text": "select * from yds1.`simple_streaming_query_input`",
            "type": "STREAMING",
            "status": "RUNNING",
            "meta": {
                "finished_at": "",
                "started_at": "ISOTIME"
            },
            "result_sets": []
        }
        assert query_json["id"] == query_id

        client.get_query(query_id)
        response = client.stop_query(query_id)
        assert response.status_code == 204

        wait_for_query_status(client, query_id, ["FAILED"])

        query_json2 = client.get_query(query_id)
        normalized_json = normalize_json(query_json2)

        normalized_json.pop("issues")
        normalized_json.pop("result_sets")

        assert normalized_json == {
            "id": "xxxxxxxxxxxxxxxxxxxx",
            "name": "my first query",
            "description": "some description",
            "text": "select * from yds1.`simple_streaming_query_input`",
            "type": "STREAMING",
            "status": "FAILED",
            "meta": {
                "finished_at": "ISOTIME",
                "started_at": "ISOTIME"
            }
        }

    def test_integral_results(self):
        client = self.create_client()
        # 2^54 = 18014398509481984
        sql = """SELECT
        100, -100,
        200l, 200ul, 10000000000ul, -20000000000l, 18014398509481984l, -18014398509481984l,
        123.5f, -789.125, 1.0 / 0.0,
        true, false,
        "hello", "hello"u,
        decimal("1.23", 6, 3),
        "he\\"llo_again"u, "Я Привет"u
        """
        result = client.create_query(sql)
        query_id = result["id"]

        wait_for_query_status(client, query_id, ["COMPLETED"])
        results_json = client.get_query_results(query_id, result_set_index=0)
        assert normalize_json(results_json) == {
            "columns": [
                {"name": "column0", "type": "Int32"}, {"name": "column1", "type": "Int32"},
                {"name": "column2", "type": "Int64"}, {"name": "column3", "type": "Uint64"},
                {"name": "column4", "type": "Uint64"}, {"name": "column5", "type": "Int64"},
                {"name": "column6", "type": "Int64"}, {"name": "column7", "type": "Int64"},
                {"name": "column8", "type": "Float"}, {"name": "column9", "type": "Double"},
                {"name": "column10", "type": "Double"}, {"name": "column11", "type": "Bool"},
                {"name": "column12", "type": "Bool"}, {"name": "column13", "type": "String"},
                {"name": "column14", "type": "Utf8"}, {"name": "column15", "type": "Decimal(6,3)"},
                {"name": "column16", "type": "Utf8"}, {"name": "column17", "type": "Utf8"}
            ],
            "rows": [
                [
                    100, -100,
                    200, 200,
                    10000000000, -20000000000,
                    "18014398509481984", "-18014398509481984",
                    123.5, -789.125,
                    "inf", True,
                    False, "aGVsbG8=",
                    "hello", "1.23",
                    "he\"llo_again", "Я Привет"
                ]
            ]
        }

        # check incorrect result set index
        results_json1 = client.get_query_results(query_id, result_set_index=1, expected_code=400)
        assert normalize_json(results_json1) == {
            "status": 400010,
            "message": "BAD_REQUEST",
            "details": [
                {
                    "issue_code": 1003,
                    "issues": [],
                    "message": "Result set index out of bound: 1 >= 1",
                    "severity": "ERROR"
                }
            ]
        }

    def test_optional_results(self):
        client = self.create_client()
        sql = """SELECT
        just(1), just(just(2)), just(just(just(3))),
        nothing(int?), just(nothing(int?)), just(just(nothing(int?)))
        """
        result = client.create_query(sql)
        query_id = result["id"]

        wait_for_query_status(client, query_id, ["COMPLETED"])
        results_json = client.get_query_results(query_id, 0)

        assert normalize_json(results_json) == {
            'columns': [
                {'name': 'column0', 'type': 'Optional<Int32>'},
                {'name': 'column1', 'type': 'Optional<Int32?>'},
                {'name': 'column2', 'type': 'Optional<Int32??>'},
                {'name': 'column3', 'type': 'Optional<Int32>'},
                {'name': 'column4', 'type': 'Optional<Int32?>'},
                {'name': 'column5', 'type': 'Optional<Int32??>'}
            ],
            'rows': [
                [
                    [1],
                    [[2]],
                    [[[3]]],
                    [],
                    [[]],
                    [[[]]]
                ]
            ]
        }

    def test_set_result(self):
        client = self.create_client()
        sql = """
        SELECT
            AsSet(1,2,3),
        """
        result = client.create_query(sql)
        query_id = result["id"]
        wait_for_query_status(client, query_id, ["COMPLETED"])
        results_json = client.get_query_results(query_id, 0)
        assert results_json["columns"] == [
            {"name": "column0", "type": "Set<Int32>"}
        ]
        data = results_json["rows"][0][0]
        data.sort()
        assert data == [1, 2, 3]

    def test_complex_results(self):
        client = self.create_client()
        sql = """
        $vt1 = ParseType("Variant<One:Int32,Two:String>");
        $vt2 = ParseType("Variant<Int32,String>");
        $vt3 = ParseType("Variant<String, Int32?>");
        $vt4 = ParseType("Variant<String, Int32??>");

        SELECT
            [], [1,2], {}, {"abc":1}, {"xyz"u:1},
            uuid("1812bc18-5838-4cde-98aa-287302697b90"),
            Interval("PT15M"),
            Date("2019-09-16"), Datetime("2019-09-16T10:46:05Z"), Timestamp("2019-09-16T11:27:44.345849Z"),
            TzDate("2019-09-16,Europe/Moscow"), TzDatetime("2019-09-16T14:32:40,Europe/Moscow"), TzTimestamp("2019-09-16T14:32:55.874913,Europe/Moscow"),
            Variant(12, "One", $vt1), Variant("xyz", "1", $vt2), AsVariant(1,"a"), AsEnum("monday"),
            AsTagged(1, "my_tag"),
            <||>, <|a:1, b:"xyz"u|>,
            void(), null,
            just(just(Variant(just(just(177)), "1", $vt4))),
            just(just(Variant(nothing(int??), "1", $vt4))),
            just(just(Variant(nothing(int?), "1", $vt3))),
        """
        result = client.create_query(sql)
        query_id = result["id"]

        wait_for_query_status(client, query_id, ["COMPLETED"])
        results_json = client.get_query_results(query_id, 0)
        # uuid, tz* types are not implemented yet
        assert results_json == {
            "columns": [
                {"name": "column0", "type": "EmptyList"},
                {"name": "column1", "type": "List<Int32>"},
                {"name": "column2", "type": "EmptyDict"},
                {"name": "column3", "type": "Dict<String,Int32>"},
                {"name": "column4", "type": "Dict<Utf8,Int32>"},
                {"name": "column5", "type": "Uuid"},
                {"name": "column6", "type": "Interval"},
                {"name": "column7", "type": "Date"},
                {"name": "column8", "type": "Datetime"},
                {"name": "column9", "type": "Timestamp"},
                {"name": "column10", "type": "TzDate"},
                {"name": "column11", "type": "TzDatetime"},
                {"name": "column12", "type": "TzTimestamp"},
                {"name": "column13", "type": "Variant<'One':Int32,'Two':String>"},
                {"name": "column14", "type": "Variant<Int32,String>"},
                {"name": "column15", "type": "Variant<'a':Int32>"},
                {"name": "column16", "type": "Enum<'monday'>"},
                {"name": "column17", "type": "Tagged<Int32,'my_tag'>"},
                {"name": "column18", "type": "Struct<>"},
                {"name": "column19", "type": "Struct<'a':Int32,'b':Utf8>"},
                {"name": "column20", "type": "Void"},
                {"name": "column21", "type": "Null"},
                {"name": "column22", "type": "Optional<Variant<String,Int32??>?>"},
                {'name': 'column23', 'type': 'Optional<Variant<String,Int32??>?>'},
                {'name': 'column24', 'type': 'Optional<Variant<String,Int32?>?>'},
            ],
            "rows": [
                [
                    [],
                    [1, 2],
                    [],
                    [["YWJj", 1]],
                    [["xyz", 1]],
                    None,  # seems like http api doesn't support uuid values
                    "PT15M",
                    "2019-09-16",
                    "2019-09-16T10:46:05Z",
                    "2019-09-16T11:27:44.345849Z",
                    "2019-09-16,Europe/Moscow",
                    "2019-09-16T14:32:40,Europe/Moscow",
                    "2019-09-16T14:32:55.874913,Europe/Moscow",
                    ["One", 12],
                    [1, "eHl6"],
                    ["a", 1],
                    ["monday", None],
                    1,
                    {},
                    {"a": 1, "b": "xyz"},
                    None,
                    None,
                    [[[1, [[177]]]]],
                    [[[1, []]]],
                    [[[1, []]]],
                ]
            ]

        }

    def test_result_offset_limit(self):
        client = self.create_client()
        sql = """
        select * from AS_TABLE([<|a:1|>, <|a:7|>, <|a:8|>, <|a:11|>]);
        """
        result = client.create_query(sql)
        query_id = result["id"]

        wait_for_query_status(client, query_id, ["COMPLETED"])
        results_json = client.get_query_results(query_id, result_set_index=0, offset=1, limit=2)
        assert results_json == {
            "columns": [
                {"name": "a", "type": "Int32"}
            ],
            "rows": [
                [7],
                [8]
            ]
        }

    def test_openapi_spec(self):
        client = self.create_client()
        spec = client.get_openapi_spec()
        assert len(spec) > 100
        parsed_spec = yaml.load(spec, Loader=SafeLoader)
        assert len(parsed_spec) > 2
        assert parsed_spec["openapi"] == "3.0.0"
