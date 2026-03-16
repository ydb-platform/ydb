import json
from urllib.parse import urlencode

import pytest

from ..utils import RepeatExecutionContext
from .app import url_string


def response_json(response):
    return json.loads(response.body.decode())


def json_dump_kwarg(**kwargs):
    return json.dumps(kwargs)


def json_dump_kwarg_list(**kwargs):
    return json.dumps([kwargs])


def test_allows_get_with_query_param(client):
    response = client.get(url_string(query="{test}"))
    assert response.status_code == 200, response.status
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_allows_get_with_variable_values(client):
    response = client.get(
        url_string(
            query="query helloWho($who: String){ test(who: $who) }",
            variables=json.dumps({"who": "Dolly"}),
        )
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_allows_get_with_operation_name(client):
    response = client.get(
        url_string(
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

    assert response.status_code == 200
    assert response_json(response) == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


def test_reports_validation_errors(client):
    response = client.get(url_string(query="{ test, unknownOne, unknownTwo }"))

    assert response.status_code == 400
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


def test_errors_when_missing_operation_name(client):
    response = client.get(
        url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """
        )
    )

    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [
            {
                "message": "Must provide operation name"
                " if query contains multiple operations.",
            }
        ]
    }


def test_errors_when_sending_a_mutation_via_get(client):
    response = client.get(
        url_string(
            query="""
        mutation TestMutation { writeTest { test } }
        """
        )
    )
    assert response.status_code == 405
    assert response_json(response) == {
        "errors": [
            {
                "message": "Can only perform a mutation operation from a POST request.",
            }
        ]
    }


def test_errors_when_selecting_a_mutation_within_a_get(client):
    response = client.get(
        url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """,
            operationName="TestMutation",
        )
    )

    assert response.status_code == 405
    assert response_json(response) == {
        "errors": [
            {
                "message": "Can only perform a mutation operation from a POST request.",
            }
        ]
    }


def test_allows_mutation_to_exist_within_a_get(client):
    response = client.get(
        url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """,
            operationName="TestQuery",
        )
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_allows_post_with_json_encoding(client):
    response = client.post(
        url_string(),
        data=json_dump_kwarg(query="{test}"),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_allows_sending_a_mutation_via_post(client):
    response = client.post(
        url_string(),
        data=json_dump_kwarg(query="mutation TestMutation { writeTest { test } }"),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"writeTest": {"test": "Hello World"}}}


def test_allows_post_with_url_encoding(client):
    response = client.post(
        url_string(),
        data=urlencode(dict(query="{test}")),
        content_type="application/x-www-form-urlencoded",
    )

    # assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello World"}}


def test_supports_post_json_query_with_string_variables(client):
    response = client.post(
        url_string(),
        data=json_dump_kwarg(
            query="query helloWho($who: String){ test(who: $who) }",
            variables=json.dumps({"who": "Dolly"}),
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_json_query_with_json_variables(client):
    response = client.post(
        url_string(),
        data=json_dump_kwarg(
            query="query helloWho($who: String){ test(who: $who) }",
            variables={"who": "Dolly"},
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_url_encoded_query_with_string_variables(client):
    response = client.post(
        url_string(),
        data=urlencode(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
                variables=json.dumps({"who": "Dolly"}),
            )
        ),
        content_type="application/x-www-form-urlencoded",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_json_quey_with_get_variable_values(client):
    response = client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data=json_dump_kwarg(
            query="query helloWho($who: String){ test(who: $who) }",
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_post_url_encoded_query_with_get_variable_values(client):
    response = client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data=urlencode(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
            )
        ),
        content_type="application/x-www-form-urlencoded",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_supports_post_raw_text_query_with_get_variable_values(client):
    response = client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data="query helloWho($who: String){ test(who: $who) }",
        content_type="application/graphql",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello Dolly"}}


def test_allows_post_with_operation_name(client):
    response = client.post(
        url_string(),
        data=json_dump_kwarg(
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
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


def test_allows_post_with_get_operation_name(client):
    response = client.post(
        url_string(operationName="helloWorld"),
        data="""
    query helloYou { test(who: "You"), ...shared }
    query helloWorld { test(who: "World"), ...shared }
    query helloDolly { test(who: "Dolly"), ...shared }
    fragment shared on QueryRoot {
      shared: test(who: "Everyone")
    }
    """,
        content_type="application/graphql",
    )

    assert response.status_code == 200
    assert response_json(response) == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


@pytest.mark.parametrize("settings", [dict(pretty=True)])
def test_supports_pretty_printing(client, settings):
    response = client.get(url_string(query="{test}"))

    assert response.body.decode() == (
        "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"
    )


@pytest.mark.parametrize("settings", [dict(pretty=False)])
def test_not_pretty_by_default(client, settings):
    response = client.get(url_string(query="{test}"))

    assert response.body.decode() == '{"data":{"test":"Hello World"}}'


def test_supports_pretty_printing_by_request(client):
    response = client.get(url_string(query="{test}", pretty="1"))

    assert response.body.decode() == (
        "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"
    )


def test_handles_field_errors_caught_by_graphql(client):
    response = client.get(url_string(query="{thrower}"))
    assert response.status_code == 200
    assert response_json(response) == {
        "data": None,
        "errors": [
            {
                "message": "Throws!",
                "locations": [{"column": 2, "line": 1}],
                "path": ["thrower"],
            }
        ],
    }


def test_handles_syntax_errors_caught_by_graphql(client):
    response = client.get(url_string(query="syntaxerror"))
    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [
            {
                "message": "Syntax Error: Unexpected Name 'syntaxerror'.",
                "locations": [{"column": 1, "line": 1}],
            }
        ]
    }


def test_handles_errors_caused_by_a_lack_of_query(client):
    response = client.get(url_string())

    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [{"message": "Must provide query string."}]
    }


def test_handles_batch_correctly_if_is_disabled(client):
    response = client.post(url_string(), data="[]", content_type="application/json")

    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [
            {
                "message": "Batch GraphQL requests are not enabled.",
            }
        ]
    }


def test_handles_incomplete_json_bodies(client):
    response = client.post(
        url_string(), data='{"query":', content_type="application/json"
    )

    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [{"message": "POST body sent invalid JSON."}]
    }


def test_handles_plain_post_text(client):
    response = client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data="query helloWho($who: String){ test(who: $who) }",
        content_type="text/plain",
    )
    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [{"message": "Must provide query string."}]
    }


def test_handles_poorly_formed_variables(client):
    response = client.get(
        url_string(
            query="query helloWho($who: String){ test(who: $who) }", variables="who:You"
        )
    )
    assert response.status_code == 400
    assert response_json(response) == {
        "errors": [{"message": "Variables are invalid JSON."}]
    }


def test_handles_unsupported_http_methods(client):
    response = client.put(url_string(query="{test}"))
    assert response.status_code == 405
    assert response.headers["Allow"] in ["GET, POST", "HEAD, GET, POST, OPTIONS"]
    assert response_json(response) == {
        "errors": [{"message": "GraphQL only supports GET and POST requests."}]
    }


def test_passes_request_into_request_context(client):
    response = client.get(url_string(query="{request}", q="testing"))

    assert response.status_code == 200
    assert response_json(response) == {"data": {"request": "testing"}}


@pytest.mark.parametrize("settings", [dict(context={"session": "CUSTOM CONTEXT"})])
def test_passes_custom_context_into_context(client, settings):
    response = client.get(url_string(query="{context { session request }}"))

    assert response.status_code == 200
    res = response_json(response)
    assert "data" in res
    assert "session" in res["data"]["context"]
    assert "request" in res["data"]["context"]
    assert "CUSTOM CONTEXT" in res["data"]["context"]["session"]
    assert "request" in res["data"]["context"]["request"]


@pytest.mark.parametrize("settings", [dict(context="CUSTOM CONTEXT")])
def test_context_remapped_if_not_mapping(client, settings):
    response = client.get(url_string(query="{context { session request }}"))

    assert response.status_code == 200
    res = response_json(response)
    assert "data" in res
    assert "session" in res["data"]["context"]
    assert "request" in res["data"]["context"]
    assert "CUSTOM CONTEXT" not in res["data"]["context"]["request"]
    assert "request" in res["data"]["context"]["request"]


class CustomContext(dict):
    property = "A custom property"


@pytest.mark.parametrize("settings", [dict(context=CustomContext())])
def test_allow_empty_custom_context(client, settings):
    response = client.get(url_string(query="{context { property request }}"))

    assert response.status_code == 200
    res = response_json(response)
    assert "data" in res
    assert "request" in res["data"]["context"]
    assert "property" in res["data"]["context"]
    assert "A custom property" == res["data"]["context"]["property"]
    assert "request" in res["data"]["context"]["request"]


def test_post_multipart_data(client):
    query = "mutation TestMutation { writeTest { test } }"
    data = (
        "------webobgraphql\r\n"
        + 'Content-Disposition: form-data; name="query"\r\n'
        + "\r\n"
        + query
        + "\r\n"
        + "------webobgraphql--\r\n"
        + "Content-Type: text/plain; charset=utf-8\r\n"
        + 'Content-Disposition: form-data; name="file"; filename="text1.txt"; filename*=utf-8\'\'text1.txt\r\n'
        + "\r\n"
        + "\r\n"
        + "------webobgraphql--\r\n"
    )

    response = client.post(
        url_string(),
        data=data,
        content_type="multipart/form-data; boundary=----webobgraphql",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"writeTest": {"test": "Hello World"}}}


@pytest.mark.parametrize("settings", [dict(batch=True)])
def test_batch_allows_post_with_json_encoding(client, settings):
    response = client.post(
        url_string(),
        data=json_dump_kwarg_list(
            # id=1,
            query="{test}"
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == [
        {
            # 'id': 1,
            "data": {"test": "Hello World"}
        }
    ]


@pytest.mark.parametrize("settings", [dict(batch=True)])
def test_batch_supports_post_json_query_with_json_variables(client, settings):
    response = client.post(
        url_string(),
        data=json_dump_kwarg_list(
            # id=1,
            query="query helloWho($who: String){ test(who: $who) }",
            variables={"who": "Dolly"},
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == [
        {
            # 'id': 1,
            "data": {"test": "Hello Dolly"}
        }
    ]


@pytest.mark.parametrize("settings", [dict(batch=True)])
def test_batch_allows_post_with_operation_name(client, settings):
    response = client.post(
        url_string(),
        data=json_dump_kwarg_list(
            # id=1,
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
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == [
        {
            # 'id': 1,
            "data": {"test": "Hello World", "shared": "Hello Everyone"}
        }
    ]


@pytest.mark.parametrize(
    "settings", [dict(execution_context_class=RepeatExecutionContext)]
)
def test_custom_execution_context_class(client):
    response = client.post(
        url_string(),
        data=json_dump_kwarg(query="{test}"),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response_json(response) == {"data": {"test": "Hello WorldHello World"}}
