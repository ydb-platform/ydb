import json
from urllib.parse import urlencode

import pytest

from ..utils import RepeatExecutionContext
from .app import create_app, url_string
from .schema import AsyncSchema


def response_json(response):
    return json.loads(response.body.decode())


def json_dump_kwarg(**kwargs):
    return json.dumps(kwargs)


def json_dump_kwarg_list(**kwargs):
    return json.dumps([kwargs])


def test_allows_get_with_query_param(app):
    _, response = app.test_client.get(uri=url_string(query="{test}"))

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_allows_get_with_variable_values(app):
    _, response = app.test_client.get(
        uri=url_string(
            query="query helloWho($who: String){ test(who: $who) }",
            variables=json.dumps({"who": "Dolly"}),
        )
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_allows_get_with_operation_name(app):
    _, response = app.test_client.get(
        uri=url_string(
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
    )

    assert response.status == 200
    assert response_json(response) == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


def test_reports_validation_errors(app):
    _, response = app.test_client.get(
        uri=url_string(query="{ test, unknownOne, unknownTwo }")
    )

    assert response.status == 400
    assert response_json(response) == {
        "errors": [
            {
                "message": "Cannot query field 'unknownOne' on type 'QueryRoot'.",
                "locations": [{"line": 1, "column": 9}],
            },
            {
                "message": "Cannot query field 'unknownTwo' on type 'QueryRoot'.",
                "locations": [{"line": 1, "column": 21}],
            },
        ]
    }


def test_errors_when_missing_operation_name(app):
    _, response = app.test_client.get(
        uri=url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """
        )
    )

    assert response.status == 400
    assert response_json(response) == {
        "errors": [
            {
                "message": "Must provide operation name"
                " if query contains multiple operations.",
            }
        ]
    }


def test_errors_when_sending_a_mutation_via_get(app):
    _, response = app.test_client.get(
        uri=url_string(
            query="""
        mutation TestMutation { writeTest { test } }
        """
        )
    )
    assert response.status == 405
    assert response_json(response) == {
        "errors": [
            {
                "message": "Can only perform a mutation operation from a POST request.",
            }
        ]
    }


def test_errors_when_selecting_a_mutation_within_a_get(app):
    _, response = app.test_client.get(
        uri=url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """,
            operationName="TestMutation",
        )
    )

    assert response.status == 405
    assert response_json(response) == {
        "errors": [
            {
                "message": "Can only perform a mutation operation from a POST request.",
            }
        ]
    }


def test_allows_mutation_to_exist_within_a_get(app):
    _, response = app.test_client.get(
        uri=url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """,
            operationName="TestQuery",
        )
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_allows_post_with_json_encoding(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg(query="{test}"),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_allows_sending_a_mutation_via_post(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg(query="mutation TestMutation { writeTest { test } }"),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"writeTest": {"test": "Hello World"}}}


def test_allows_post_with_url_encoding(app):
    # Example of how sanic does send data using url enconding
    # can be found at their repo.
    # https://github.com/huge-success/sanic/blob/master/tests/test_requests.py#L927
    payload = "query={test}"
    _, response = app.test_client.post(
        uri=url_string(),
        content=payload,
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_supports_post_json_query_with_string_variables(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg(
            query="query helloWho($who: String){ test(who: $who) }",
            variables=json.dumps({"who": "Dolly"}),
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_json_query_with_json_variables(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg(
            query="query helloWho($who: String){ test(who: $who) }",
            variables={"who": "Dolly"},
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_url_encoded_query_with_string_variables(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=urlencode(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
                variables=json.dumps({"who": "Dolly"}),
            )
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_json_query_with_get_variable_values(app):
    _, response = app.test_client.post(
        uri=url_string(variables=json.dumps({"who": "Dolly"})),
        content=json_dump_kwarg(
            query="query helloWho($who: String){ test(who: $who) }",
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_post_url_encoded_query_with_get_variable_values(app):
    _, response = app.test_client.post(
        uri=url_string(variables=json.dumps({"who": "Dolly"})),
        content=urlencode(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
            )
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_raw_text_query_with_get_variable_values(app):
    _, response = app.test_client.post(
        uri=url_string(variables=json.dumps({"who": "Dolly"})),
        content="query helloWho($who: String){ test(who: $who) }",
        headers={"content-type": "application/graphql"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_allows_post_with_operation_name(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg(
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
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


def test_allows_post_with_get_operation_name(app):
    _, response = app.test_client.post(
        uri=url_string(operationName="helloWorld"),
        content="""
    query helloYou { test(who: "You"), ...shared }
    query helloWorld { test(who: "World"), ...shared }
    query helloDolly { test(who: "Dolly"), ...shared }
    fragment shared on QueryRoot {
      shared: test(who: "Everyone")
    }
    """,
        headers={"content-type": "application/graphql"},
    )

    assert response.status == 200
    assert response_json(response) == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


@pytest.mark.parametrize("app", [create_app(pretty=True)])
def test_supports_pretty_printing(app):
    _, response = app.test_client.get(uri=url_string(query="{test}"))

    assert response.body.decode() == (
        "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"
    )


@pytest.mark.parametrize("app", [create_app(pretty=False)])
def test_not_pretty_by_default(app):
    _, response = app.test_client.get(url_string(query="{test}"))

    assert response.body.decode() == '{"data":{"test":"Hello World"}}'


def test_supports_pretty_printing_by_request(app):
    _, response = app.test_client.get(uri=url_string(query="{test}", pretty="1"))

    assert response.body.decode() == (
        "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"
    )


def test_handles_field_errors_caught_by_graphql(app):
    _, response = app.test_client.get(uri=url_string(query="{thrower}"))
    assert response.status == 200
    assert response_json(response) == {
        "data": None,
        "errors": [
            {
                "locations": [{"column": 2, "line": 1}],
                "message": "Throws!",
                "path": ["thrower"],
            }
        ],
    }


def test_handles_syntax_errors_caught_by_graphql(app):
    _, response = app.test_client.get(uri=url_string(query="syntaxerror"))
    assert response.status == 400
    assert response_json(response) == {
        "errors": [
            {
                "locations": [{"column": 1, "line": 1}],
                "message": "Syntax Error: Unexpected Name 'syntaxerror'.",
            }
        ]
    }


def test_handles_errors_caused_by_a_lack_of_query(app):
    _, response = app.test_client.get(uri=url_string())

    assert response.status == 400
    assert response_json(response) == {
        "errors": [{"message": "Must provide query string."}]
    }


def test_handles_batch_correctly_if_is_disabled(app):
    _, response = app.test_client.post(
        uri=url_string(), content="[]", headers={"content-type": "application/json"}
    )

    assert response.status == 400
    assert response_json(response) == {
        "errors": [
            {
                "message": "Batch GraphQL requests are not enabled.",
            }
        ]
    }


def test_handles_incomplete_json_bodies(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content='{"query":',
        headers={"content-type": "application/json"},
    )

    assert response.status == 400
    assert response_json(response) == {
        "errors": [{"message": "POST body sent invalid JSON."}]
    }


def test_handles_plain_post_text(app):
    _, response = app.test_client.post(
        uri=url_string(variables=json.dumps({"who": "Dolly"})),
        content="query helloWho($who: String){ test(who: $who) }",
        headers={"content-type": "text/plain"},
    )
    assert response.status == 400
    assert response_json(response) == {
        "errors": [{"message": "Must provide query string."}]
    }


def test_handles_poorly_formed_variables(app):
    _, response = app.test_client.get(
        uri=url_string(
            query="query helloWho($who: String){ test(who: $who) }", variables="who:You"
        )
    )
    assert response.status == 400
    assert response_json(response) == {
        "errors": [{"message": "Variables are invalid JSON."}]
    }


def test_handles_unsupported_http_methods(app):
    _, response = app.test_client.put(uri=url_string(query="{test}"))
    assert response.status == 405
    allowed_methods = set(
        method.strip() for method in response.headers["Allow"].split(",")
    )
    assert allowed_methods in [{"GET", "POST"}, {"HEAD", "GET", "POST", "OPTIONS"}]
    assert response_json(response) == {
        "errors": [
            {
                "message": "GraphQL only supports GET and POST requests.",
            }
        ]
    }


def test_passes_request_into_request_context(app):
    _, response = app.test_client.get(uri=url_string(query="{request}", q="testing"))

    assert response.status == 200
    assert response_json(response) == {"data": {"request": "testing"}}


@pytest.mark.parametrize("app", [create_app(context={"session": "CUSTOM CONTEXT"})])
def test_passes_custom_context_into_context(app):
    _, response = app.test_client.get(
        uri=url_string(query="{context { session request }}")
    )

    assert response.status_code == 200
    res = response_json(response)
    assert "data" in res
    assert "session" in res["data"]["context"]
    assert "request" in res["data"]["context"]
    assert "CUSTOM CONTEXT" in res["data"]["context"]["session"]
    assert "Request" in res["data"]["context"]["request"]


class CustomContext(dict):
    property = "A custom property"


@pytest.mark.parametrize("app", [create_app(context=CustomContext())])
def test_allow_empty_custom_context(app):
    _, response = app.test_client.get(
        uri=url_string(query="{context { property request }}")
    )

    assert response.status_code == 200
    res = response_json(response)
    assert "data" in res
    assert "request" in res["data"]["context"]
    assert "property" in res["data"]["context"]
    assert "A custom property" == res["data"]["context"]["property"]
    assert "Request" in res["data"]["context"]["request"]


@pytest.mark.parametrize("app", [create_app(context="CUSTOM CONTEXT")])
def test_context_remapped_if_not_mapping(app):
    _, response = app.test_client.get(
        uri=url_string(query="{context { session request }}")
    )

    assert response.status_code == 200
    res = response_json(response)
    assert "data" in res
    assert "session" in res["data"]["context"]
    assert "request" in res["data"]["context"]
    assert "CUSTOM CONTEXT" not in res["data"]["context"]["request"]
    assert "Request" in res["data"]["context"]["request"]


def test_post_multipart_data(app):
    query = "mutation TestMutation { writeTest { test } }"

    data = (
        "------sanicgraphql\r\n"
        + 'Content-Disposition: form-data; name="query"\r\n'
        + "\r\n"
        + query
        + "\r\n"
        + "------sanicgraphql--\r\n"
        + "Content-Type: text/plain; charset=utf-8\r\n"
        + 'Content-Disposition: form-data; name="file"; filename="text1.txt"; filename*=utf-8\'\'text1.txt\r\n'
        + "\r\n"
        + "\r\n"
        + "------sanicgraphql--\r\n"
    )

    _, response = app.test_client.post(
        uri=url_string(),
        content=data,
        headers={"content-type": "multipart/form-data; boundary=----sanicgraphql"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"writeTest": {"test": "Hello World"}}}


@pytest.mark.parametrize("app", [create_app(batch=True)])
def test_batch_allows_post_with_json_encoding(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg_list(id=1, query="{test}"),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == [{"data": {"test": "Hello World"}}]


@pytest.mark.parametrize("app", [create_app(batch=True)])
def test_batch_supports_post_json_query_with_json_variables(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg_list(
            id=1,
            query="query helloWho($who: String){ test(who: $who) }",
            variables={"who": "Dolly"},
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == [{"data": {"test": "Hello Dolly"}}]


@pytest.mark.parametrize("app", [create_app(batch=True)])
def test_batch_allows_post_with_operation_name(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg_list(
            id=1,
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
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == [
        {"data": {"test": "Hello World", "shared": "Hello Everyone"}}
    ]


@pytest.mark.parametrize("app", [create_app(schema=AsyncSchema, enable_async=True)])
def test_async_schema(app):
    query = "{a,b,c}"
    _, response = app.test_client.get(uri=url_string(query=query))

    assert response.status == 200
    assert response_json(response) == {"data": {"a": "hey", "b": "hey2", "c": "hey3"}}


def test_preflight_request(app):
    _, response = app.test_client.options(
        uri=url_string(), headers={"Access-Control-Request-Method": "POST"}
    )

    assert response.status == 200


def test_preflight_incorrect_request(app):
    _, response = app.test_client.options(
        uri=url_string(), headers={"Access-Control-Request-Method": "OPTIONS"}
    )

    assert response.status == 400


@pytest.mark.parametrize(
    "app", [create_app(execution_context_class=RepeatExecutionContext)]
)
def test_custom_execution_context_class(app):
    _, response = app.test_client.post(
        uri=url_string(),
        content=json_dump_kwarg(query="{test}"),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert response_json(response) == {"data": {"test": "Hello WorldHello World"}}
