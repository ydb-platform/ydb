import json

from graphql.error import GraphQLError
from graphql.execution import ExecutionResult
from graphql.validation import ValidationRule
from pytest import raises

from graphql_server import (
    GraphQLParams,
    GraphQLResponse,
    HttpQueryError,
    encode_execution_results,
    format_execution_result,
    json_encode,
    load_json_body,
    run_http_query,
)
from graphql_server.render_graphiql import (
    GraphiQLConfig,
    GraphiQLData,
    render_graphiql_sync,
)

from .schema import invalid_schema, schema
from .utils import as_dicts


def test_request_params():
    assert issubclass(GraphQLParams, tuple)
    # noinspection PyUnresolvedReferences
    assert GraphQLParams._fields == ("query", "variables", "operation_name")


def test_server_results():
    assert issubclass(GraphQLResponse, tuple)
    # noinspection PyUnresolvedReferences
    assert GraphQLResponse._fields == ("results", "params")


def test_validate_schema():
    query = "{test}"
    results, params = run_http_query(invalid_schema, "get", {}, dict(query=query))
    assert as_dicts(results) == [
        {
            "data": None,
            "errors": [
                {
                    "message": "Query root type must be provided.",
                }
            ],
        }
    ]


def test_allows_get_with_query_param():
    query = "{test}"
    results, params = run_http_query(schema, "get", {}, dict(query=query))

    assert as_dicts(results) == [{"data": {"test": "Hello World"}, "errors": None}]
    assert params == [GraphQLParams(query=query, variables=None, operation_name=None)]


def test_allows_get_with_variable_values():
    results, params = run_http_query(
        schema,
        "get",
        {},
        dict(
            query="query helloWho($who: String){ test(who: $who) }",
            variables=json.dumps({"who": "Dolly"}),
        ),
    )

    assert as_dicts(results) == [{"data": {"test": "Hello Dolly"}, "errors": None}]


def test_allows_get_with_operation_name():
    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(
            query="""
        query helloYou { test(who: "You"), ...shared }
        query helloWorld { test(who: "World"), ...shared }
        query helloDolly { test(who: "Dolly"), ...shared }
        fragment shared on QueryRoot {
          shared: test(who: "Everyone")
        }
        """,
            operationName="helloWorld",
        ),
    )

    assert as_dicts(results) == [
        {"data": {"test": "Hello World", "shared": "Hello Everyone"}, "errors": None}
    ]

    response = encode_execution_results(results)
    assert response.status_code == 200


def test_reports_validation_errors():
    results, params = run_http_query(
        schema, "get", {}, query_data=dict(query="{ test, unknownOne, unknownTwo }")
    )

    assert as_dicts(results) == [
        {
            "data": None,
            "errors": [
                {
                    "message": "Cannot query field 'unknownOne' on type 'QueryRoot'.",
                    "locations": [{"line": 1, "column": 9}],
                },
                {
                    "message": "Cannot query field 'unknownTwo' on type 'QueryRoot'.",
                    "locations": [{"line": 1, "column": 21}],
                },
            ],
        }
    ]

    response = encode_execution_results(results)
    assert response.status_code == 400


def test_reports_custom_validation_errors():
    class CustomValidationRule(ValidationRule):
        def enter_field(self, node, *_args):
            self.report_error(GraphQLError("Custom validation error.", node))

    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(query="{ test }"),
        validation_rules=[CustomValidationRule],
    )

    assert as_dicts(results) == [
        {
            "data": None,
            "errors": [
                {
                    "message": "Custom validation error.",
                    "locations": [{"line": 1, "column": 3}],
                }
            ],
        }
    ]

    response = encode_execution_results(results)
    assert response.status_code == 400


def test_reports_max_num_of_validation_errors():
    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(query="{ test, unknownOne, unknownTwo }"),
        max_errors=1,
    )

    assert as_dicts(results) == [
        {
            "data": None,
            "errors": [
                {
                    "message": "Cannot query field 'unknownOne' on type 'QueryRoot'.",
                    "locations": [{"line": 1, "column": 9}],
                },
                {
                    "message": "Too many validation errors, error limit reached."
                    " Validation aborted.",
                },
            ],
        }
    ]

    response = encode_execution_results(results)
    assert response.status_code == 400


def test_non_dict_params_in_non_batch_query():
    with raises(HttpQueryError) as exc_info:
        # noinspection PyTypeChecker
        run_http_query(schema, "get", "not a dict")  # type: ignore

    assert exc_info.value == HttpQueryError(
        400, "GraphQL params should be a dict. Received 'not a dict'."
    )


def test_empty_batch_in_batch_query():
    with raises(HttpQueryError) as exc_info:
        run_http_query(schema, "get", [], batch_enabled=True)

    assert exc_info.value == HttpQueryError(
        400, "Received an empty list in the batch request."
    )


def test_errors_when_missing_operation_name():
    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """
        ),
    )

    assert as_dicts(results) == [
        {
            "data": None,
            "errors": [
                {
                    "message": (
                        "Must provide operation name"
                        " if query contains multiple operations."
                    ),
                }
            ],
        }
    ]
    assert isinstance(results[0].errors[0], GraphQLError)


def test_errors_when_sending_a_mutation_via_get():
    with raises(HttpQueryError) as exc_info:
        run_http_query(
            schema,
            "get",
            {},
            query_data=dict(
                query="""
                mutation TestMutation { writeTest { test } }
                """
            ),
        )

    assert exc_info.value == HttpQueryError(
        405,
        "Can only perform a mutation operation from a POST request.",
        headers={"Allow": "POST"},
    )


def test_catching_errors_when_sending_a_mutation_via_get():
    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(
            query="""
                mutation TestMutation { writeTest { test } }
                """
        ),
        catch=True,
    )

    assert results == [None]


def test_errors_when_selecting_a_mutation_within_a_get():
    with raises(HttpQueryError) as exc_info:
        run_http_query(
            schema,
            "get",
            {},
            query_data=dict(
                query="""
                query TestQuery { test }
                mutation TestMutation { writeTest { test } }
                """,
                operationName="TestMutation",
            ),
        )

    assert exc_info.value == HttpQueryError(
        405,
        "Can only perform a mutation operation from a POST request.",
        headers={"Allow": "POST"},
    )


def test_allows_mutation_to_exist_within_a_get():
    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(
            query="""
            query TestQuery { test }
            mutation TestMutation { writeTest { test } }
            """,
            operationName="TestQuery",
        ),
    )

    assert as_dicts(results) == [{"data": {"test": "Hello World"}, "errors": None}]


def test_allows_sending_a_mutation_via_post():
    results, params = run_http_query(
        schema,
        "post",
        {},
        query_data=dict(query="mutation TestMutation { writeTest { test } }"),
    )

    assert results == [({"writeTest": {"test": "Hello World"}}, None)]


def test_allows_post_with_url_encoding():
    results, params = run_http_query(
        schema, "post", {}, query_data=dict(query="{test}")
    )

    assert results == [({"test": "Hello World"}, None)]


def test_supports_post_json_query_with_string_variables():
    results, params = run_http_query(
        schema,
        "post",
        {},
        query_data=dict(
            query="query helloWho($who: String){ test(who: $who) }",
            variables='{"who": "Dolly"}',
        ),
    )

    assert results == [({"test": "Hello Dolly"}, None)]


def test_supports_post_json_query_with_json_variables():
    result = load_json_body(
        """
        {
            "query": "query helloWho($who: String){ test(who: $who) }",
            "variables": {"who": "Dolly"}
        }
        """
    )

    assert result["variables"] == {"who": "Dolly"}


def test_supports_post_url_encoded_query_with_string_variables():
    results, params = run_http_query(
        schema,
        "post",
        {},
        query_data=dict(
            query="query helloWho($who: String){ test(who: $who) }",
            variables='{"who": "Dolly"}',
        ),
    )

    assert results == [({"test": "Hello Dolly"}, None)]


def test_supports_post_json_query_with_get_variable_values():
    results, params = run_http_query(
        schema,
        "post",
        data=dict(query="query helloWho($who: String){ test(who: $who) }"),
        query_data=dict(variables={"who": "Dolly"}),
    )

    assert results == [({"test": "Hello Dolly"}, None)]


def test_post_url_encoded_query_with_get_variable_values():
    results, params = run_http_query(
        schema,
        "get",
        data=dict(query="query helloWho($who: String){ test(who: $who) }"),
        query_data=dict(variables='{"who": "Dolly"}'),
    )

    assert results == [({"test": "Hello Dolly"}, None)]


def test_supports_post_raw_text_query_with_get_variable_values():
    results, params = run_http_query(
        schema,
        "get",
        data=dict(query="query helloWho($who: String){ test(who: $who) }"),
        query_data=dict(variables='{"who": "Dolly"}'),
    )

    assert results == [({"test": "Hello Dolly"}, None)]


def test_allows_post_with_operation_name():
    results, params = run_http_query(
        schema,
        "get",
        data=dict(
            query="""
            query helloYou { test(who: "You"), ...shared }
            query helloWorld { test(who: "World"), ...shared }
            query helloDolly { test(who: "Dolly"), ...shared }
            fragment shared on QueryRoot {
              shared: test(who: "Everyone")
            }
            """,
            operationName="helloWorld",
        ),
    )

    assert results == [({"test": "Hello World", "shared": "Hello Everyone"}, None)]


def test_allows_post_with_get_operation_name():
    results, params = run_http_query(
        schema,
        "get",
        data=dict(
            query="""
            query helloYou { test(who: "You"), ...shared }
            query helloWorld { test(who: "World"), ...shared }
            query helloDolly { test(who: "Dolly"), ...shared }
            fragment shared on QueryRoot {
              shared: test(who: "Everyone")
            }
            """
        ),
        query_data=dict(operationName="helloWorld"),
    )

    assert results == [({"test": "Hello World", "shared": "Hello Everyone"}, None)]


def test_supports_pretty_printing_data():
    results, params = run_http_query(schema, "get", data=dict(query="{test}"))
    result = {"data": results[0].data}

    assert json_encode(result, pretty=True) == (
        "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"
    )


def test_not_pretty_data_by_default():
    results, params = run_http_query(schema, "get", data=dict(query="{test}"))
    result = {"data": results[0].data}

    assert json_encode(result) == '{"data":{"test":"Hello World"}}'


def test_handles_field_errors_caught_by_graphql():
    results, params = run_http_query(schema, "get", data=dict(query="{thrower}"))

    assert results == [
        (None, [{"message": "Throws!", "locations": [(1, 2)], "path": ["thrower"]}])
    ]

    response = encode_execution_results(results)
    assert response.status_code == 200


def test_handles_syntax_errors_caught_by_graphql():
    results, params = run_http_query(schema, "get", data=dict(query="syntaxerror"))

    assert results == [
        (
            None,
            [
                {
                    "locations": [(1, 1)],
                    "message": "Syntax Error: Unexpected Name 'syntaxerror'.",
                }
            ],
        )
    ]


def test_handles_errors_caused_by_a_lack_of_query():
    with raises(HttpQueryError) as exc_info:
        run_http_query(schema, "get", {})

    assert exc_info.value == HttpQueryError(400, "Must provide query string.")


def test_handles_errors_caused_by_invalid_query_type():
    with raises(HttpQueryError) as exc_info:
        results, params = run_http_query(schema, "get", dict(query=42))

    assert exc_info.value == HttpQueryError(400, "Unexpected query type.")


def test_handles_batch_correctly_if_is_disabled():
    with raises(HttpQueryError) as exc_info:
        run_http_query(schema, "post", [])

    assert exc_info.value == HttpQueryError(
        400, "Batch GraphQL requests are not enabled."
    )


def test_handles_incomplete_json_bodies():
    with raises(HttpQueryError) as exc_info:
        run_http_query(schema, "post", load_json_body('{"query":'))

    assert exc_info.value == HttpQueryError(400, "POST body sent invalid JSON.")


def test_handles_plain_post_text():
    with raises(HttpQueryError) as exc_info:
        run_http_query(schema, "post", {})

    assert exc_info.value == HttpQueryError(400, "Must provide query string.")


def test_handles_poorly_formed_variables():
    with raises(HttpQueryError) as exc_info:
        run_http_query(
            schema,
            "get",
            {},
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
                variables="who:You",
            ),
        )

    assert exc_info.value == HttpQueryError(400, "Variables are invalid JSON.")


def test_handles_bad_schema():
    with raises(TypeError) as exc_info:
        # noinspection PyTypeChecker
        run_http_query("not a schema", "get", {})  # type: ignore

    assert str(exc_info.value) == (
        "Expected a GraphQL schema, but received 'not a schema'."
    )


def test_handles_unsupported_http_methods():
    with raises(HttpQueryError) as exc_info:
        run_http_query(schema, "put", {})

    assert exc_info.value == HttpQueryError(
        405,
        "GraphQL only supports GET and POST requests.",
        headers={"Allow": "GET, POST"},
    )


def test_format_execution_result():
    result = format_execution_result(None)
    assert result == GraphQLResponse(None, 200)
    data = {"answer": 42}
    result = format_execution_result(ExecutionResult(data, None))
    assert result == GraphQLResponse({"data": data}, 200)
    errors = [GraphQLError("bad")]
    result = format_execution_result(ExecutionResult(None, errors))
    assert result == GraphQLResponse({"errors": errors}, 400)


def test_encode_execution_results():
    data = {"answer": 42}
    errors = [GraphQLError("bad")]
    results = [ExecutionResult(data, None), ExecutionResult(None, errors)]
    result = encode_execution_results(results)
    assert result == ('{"data":{"answer":42}}', 400)


def test_encode_execution_results_batch():
    data = {"answer": 42}
    errors = [GraphQLError("bad")]
    results = [ExecutionResult(data, None), ExecutionResult(None, errors)]
    result = encode_execution_results(results, is_batch=True)
    assert result == (
        '[{"data":{"answer":42}},{"errors":[{"message":"bad"}]}]',
        400,
    )


def test_encode_execution_results_not_encoded():
    data = {"answer": 42}
    results = [ExecutionResult(data, None)]
    result = encode_execution_results(results, encode=lambda r: r)
    assert result == ({"data": data}, 200)


def test_passes_request_into_request_context():
    results, params = run_http_query(
        schema,
        "get",
        {},
        query_data=dict(query="{request}"),
        context_value={"q": "testing"},
    )

    assert results == [({"request": "testing"}, None)]


def test_supports_pretty_printing_context():
    class Context:
        def __str__(self):
            return "CUSTOM CONTEXT"

    results, params = run_http_query(
        schema, "get", {}, query_data=dict(query="{context}"), context_value=Context()
    )

    assert results == [({"context": "CUSTOM CONTEXT"}, None)]


def test_post_multipart_data():
    query = "mutation TestMutation { writeTest { test } }"
    results, params = run_http_query(schema, "post", {}, query_data=dict(query=query))

    assert results == [({"writeTest": {"test": "Hello World"}}, None)]


def test_batch_allows_post_with_json_encoding():
    data = load_json_body('[{"query": "{test}"}]')
    results, params = run_http_query(schema, "post", data, batch_enabled=True)

    assert results == [({"test": "Hello World"}, None)]


def test_batch_supports_post_json_query_with_json_variables():
    data = load_json_body(
        '[{"query":"query helloWho($who: String){ test(who: $who) }",'
        '"variables":{"who":"Dolly"}}]'
    )
    results, params = run_http_query(schema, "post", data, batch_enabled=True)

    assert results == [({"test": "Hello Dolly"}, None)]


def test_batch_allows_post_with_operation_name():
    data = [
        dict(
            query="""
            query helloYou { test(who: "You"), ...shared }
            query helloWorld { test(who: "World"), ...shared }
            query helloDolly { test(who: "Dolly"), ...shared }
            fragment shared on QueryRoot {
              shared: test(who: "Everyone")
            }
            """,
            operationName="helloWorld",
        )
    ]
    data = load_json_body(json_encode(data))
    results, params = run_http_query(schema, "post", data, batch_enabled=True)

    assert results == [({"test": "Hello World", "shared": "Hello Everyone"}, None)]


def test_graphiql_render_umlaut():
    results, params = run_http_query(
        schema,
        "get",
        data=dict(query="query helloWho($who: String){ test(who: $who) }"),
        query_data=dict(variables='{"who": "Björn"}'),
        catch=True,
    )
    result, status_code = encode_execution_results(results)

    assert status_code == 200

    graphiql_data = GraphiQLData(result=result, query=params[0].query)
    source = render_graphiql_sync(data=graphiql_data, config=GraphiQLConfig())

    assert "Hello Bj\\\\u00f6rn" in source
