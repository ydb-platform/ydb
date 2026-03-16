import json

from graphql import Source
from graphql.error import GraphQLError
from graphql.execution import ExecutionResult
from pytest import raises

from graphql_server import (
    HttpQueryError,
    ServerResponse,
    encode_execution_results,
    json_encode,
    json_encode_pretty,
    load_json_body,
)


def test_json_encode():
    result = json_encode({"query": "{test}"})
    assert result == '{"query":"{test}"}'


def test_json_encode_with_pretty_argument():
    result = json_encode({"query": "{test}"}, pretty=False)
    assert result == '{"query":"{test}"}'
    result = json_encode({"query": "{test}"}, pretty=True)
    assert result == '{\n  "query": "{test}"\n}'


def test_load_json_body_as_dict():
    result = load_json_body('{"query": "{test}"}')
    assert result == {"query": "{test}"}


def test_load_json_body_with_variables():
    result = load_json_body(
        """
        {
            "query": "query helloWho($who: String){ test(who: $who) }",
            "variables": {"who": "Dolly"}
        }
        """
    )

    assert result["variables"] == {"who": "Dolly"}


def test_load_json_body_as_list():
    result = load_json_body('[{"query": "{test}"}]')
    assert result == [{"query": "{test}"}]


def test_load_invalid_json_body():
    with raises(HttpQueryError) as exc_info:
        load_json_body('{"query":')
    assert exc_info.value == HttpQueryError(400, "POST body sent invalid JSON.")


def test_graphql_server_response():
    assert issubclass(ServerResponse, tuple)
    # noinspection PyUnresolvedReferences
    assert ServerResponse._fields == ("body", "status_code")


def test_encode_execution_results_without_error():
    execution_results = [
        ExecutionResult({"result": 1}, None),
        ExecutionResult({"result": 2}, None),
        ExecutionResult({"result": 3}, None),
    ]

    output = encode_execution_results(execution_results)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert json.loads(output.body) == {"data": {"result": 1}}
    assert output.status_code == 200


def test_encode_execution_results_with_error():
    execution_results = [
        ExecutionResult(
            None,
            [
                GraphQLError(
                    "Some error",
                    source=Source(body="Some error"),
                    positions=[1],
                    path=["somePath"],
                )
            ],
        ),
        ExecutionResult({"result": 42}, None),
    ]

    output = encode_execution_results(execution_results)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert json.loads(output.body) == {
        "errors": [
            {
                "message": "Some error",
                "locations": [{"line": 1, "column": 2}],
                "path": ["somePath"],
            }
        ],
        "data": None,
    }
    assert output.status_code == 200


def test_encode_execution_results_with_empty_result():
    execution_results = [None]

    output = encode_execution_results(execution_results)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert output.body == "null"
    assert output.status_code == 200


def test_encode_execution_results_with_format_error():
    execution_results = [
        ExecutionResult(
            None,
            [
                GraphQLError(
                    "Some msg",
                    source=Source("Some msg"),
                    positions=[1],
                    path=["some", "path"],
                )
            ],
        )
    ]

    def format_error(error):
        return {
            "msg": error.message,
            "loc": "{}:{}".format(error.locations[0].line, error.locations[0].column),
            "pth": "/".join(error.path),
        }

    output = encode_execution_results(execution_results, format_error=format_error)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert json.loads(output.body) == {
        "errors": [{"msg": "Some msg", "loc": "1:2", "pth": "some/path"}],
        "data": None,
    }
    assert output.status_code == 200


def test_encode_execution_results_with_batch():
    execution_results = [
        ExecutionResult({"result": 1}, None),
        ExecutionResult({"result": 2}, None),
        ExecutionResult({"result": 3}, None),
    ]

    output = encode_execution_results(execution_results, is_batch=True)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert json.loads(output.body) == [
        {"data": {"result": 1}},
        {"data": {"result": 2}},
        {"data": {"result": 3}},
    ]
    assert output.status_code == 200


def test_encode_execution_results_with_batch_and_empty_result():
    execution_results = [
        ExecutionResult({"result": 1}, None),
        None,
        ExecutionResult({"result": 3}, None),
    ]

    output = encode_execution_results(execution_results, is_batch=True)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert json.loads(output.body) == [
        {"data": {"result": 1}},
        None,
        {"data": {"result": 3}},
    ]
    assert output.status_code == 200


def test_encode_execution_results_with_encode():
    execution_results = [ExecutionResult({"result": None}, None)]

    def encode(result):
        return repr(dict(result))

    output = encode_execution_results(execution_results, encode=encode)
    assert isinstance(output, ServerResponse)
    assert isinstance(output.body, str)
    assert isinstance(output.status_code, int)
    assert output.body == "{'data': {'result': None}}"
    assert output.status_code == 200


def test_encode_execution_results_with_pretty_encode():
    execution_results = [ExecutionResult({"test": "Hello World"}, None)]

    output = encode_execution_results(execution_results, encode=json_encode_pretty)
    body = output.body
    assert body == "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"


def test_encode_execution_results_not_pretty_by_default():
    execution_results = [ExecutionResult({"test": "Hello World"}, None)]
    # execution_results = [ExecutionResult({"result": None}, None)]

    output = encode_execution_results(execution_results)
    assert output.body == '{"data":{"test":"Hello World"}}'
