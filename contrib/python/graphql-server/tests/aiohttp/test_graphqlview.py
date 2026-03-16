import json
from urllib.parse import urlencode

import pytest
from aiohttp import FormData

from ..utils import RepeatExecutionContext
from .app import create_app, url_string
from .schema import AsyncSchema


@pytest.mark.asyncio
async def test_allows_get_with_query_param(client):
    response = await client.get(url_string(query="{test}"))

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello World"}}


@pytest.mark.asyncio
async def test_allows_get_with_variable_values(client):
    response = await client.get(
        url_string(
            query="query helloWho($who: String) { test(who: $who) }",
            variables=json.dumps({"who": "Dolly"}),
        )
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_allows_get_with_operation_name(client):
    response = await client.get(
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

    assert response.status == 200
    assert await response.json() == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


@pytest.mark.asyncio
async def test_reports_validation_errors(client):
    response = await client.get(url_string(query="{ test, unknownOne, unknownTwo }"))

    assert response.status == 400
    assert await response.json() == {
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


@pytest.mark.asyncio
async def test_errors_when_missing_operation_name(client):
    response = await client.get(
        url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        subscription TestSubscriptions { subscriptionsTest { test } }
        """
        )
    )

    assert response.status == 400
    assert await response.json() == {
        "errors": [
            {
                "message": (
                    "Must provide operation name if query contains multiple "
                    "operations."
                ),
            },
        ]
    }


@pytest.mark.asyncio
async def test_errors_when_sending_a_mutation_via_get(client):
    response = await client.get(
        url_string(
            query="""
        mutation TestMutation { writeTest { test } }
        """
        )
    )
    assert response.status == 405
    assert await response.json() == {
        "errors": [
            {
                "message": "Can only perform a mutation operation from a POST request.",
            },
        ],
    }


@pytest.mark.asyncio
async def test_errors_when_selecting_a_mutation_within_a_get(client):
    response = await client.get(
        url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """,
            operationName="TestMutation",
        )
    )

    assert response.status == 405
    assert await response.json() == {
        "errors": [
            {
                "message": "Can only perform a mutation operation from a POST request.",
            },
        ],
    }


@pytest.mark.asyncio
async def test_errors_when_selecting_a_subscription_within_a_get(client):
    response = await client.get(
        url_string(
            query="""
        subscription TestSubscriptions { subscriptionsTest { test } }
        """,
            operationName="TestSubscriptions",
        )
    )

    assert response.status == 405
    assert await response.json() == {
        "errors": [
            {
                "message": "Can only perform a subscription operation"
                " from a POST request.",
            },
        ],
    }


@pytest.mark.asyncio
async def test_allows_mutation_to_exist_within_a_get(client):
    response = await client.get(
        url_string(
            query="""
        query TestQuery { test }
        mutation TestMutation { writeTest { test } }
        """,
            operationName="TestQuery",
        )
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello World"}}


@pytest.mark.asyncio
async def test_allows_post_with_json_encoding(client):
    response = await client.post(
        "/graphql",
        data=json.dumps(dict(query="{test}")),
        headers={"content-type": "application/json"},
    )

    assert await response.json() == {"data": {"test": "Hello World"}}
    assert response.status == 200


@pytest.mark.asyncio
async def test_allows_sending_a_mutation_via_post(client):
    response = await client.post(
        "/graphql",
        data=json.dumps(
            dict(
                query="mutation TestMutation { writeTest { test } }",
            )
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"writeTest": {"test": "Hello World"}}}


@pytest.mark.asyncio
async def test_allows_post_with_url_encoding(client):
    data = FormData()
    data.add_field("query", "{test}")
    response = await client.post(
        "/graphql",
        data=data(),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert await response.json() == {"data": {"test": "Hello World"}}
    assert response.status == 200


@pytest.mark.asyncio
async def test_supports_post_json_query_with_string_variables(client):
    response = await client.post(
        "/graphql",
        data=json.dumps(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
                variables=json.dumps({"who": "Dolly"}),
            )
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_supports_post_json_query_with_json_variables(client):
    response = await client.post(
        "/graphql",
        data=json.dumps(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
                variables={"who": "Dolly"},
            )
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_supports_post_url_encoded_query_with_string_variables(client):
    response = await client.post(
        "/graphql",
        data=urlencode(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
                variables=json.dumps({"who": "Dolly"}),
            ),
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_supports_post_json_quey_with_get_variable_values(client):
    response = await client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data=json.dumps(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
            )
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_post_url_encoded_query_with_get_variable_values(client):
    response = await client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data=urlencode(
            dict(
                query="query helloWho($who: String){ test(who: $who) }",
            )
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_supports_post_raw_text_query_with_get_variable_values(client):
    response = await client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data="query helloWho($who: String){ test(who: $who) }",
        headers={"content-type": "application/graphql"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello Dolly"}}


@pytest.mark.asyncio
async def test_allows_post_with_operation_name(client):
    response = await client.post(
        "/graphql",
        data=json.dumps(
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
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


@pytest.mark.asyncio
async def test_allows_post_with_get_operation_name(client):
    response = await client.post(
        url_string(operationName="helloWorld"),
        data="""
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
    assert await response.json() == {
        "data": {"test": "Hello World", "shared": "Hello Everyone"}
    }


@pytest.mark.asyncio
async def test_supports_pretty_printing(client):
    response = await client.get(url_string(query="{test}", pretty="1"))

    text = await response.text()
    assert text == "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"


@pytest.mark.asyncio
async def test_not_pretty_by_default(client):
    response = await client.get(url_string(query="{test}"))

    assert await response.text() == '{"data":{"test":"Hello World"}}'


@pytest.mark.asyncio
async def test_supports_pretty_printing_by_request(client):
    response = await client.get(url_string(query="{test}", pretty="1"))

    assert await response.text() == (
        "{\n" '  "data": {\n' '    "test": "Hello World"\n' "  }\n" "}"
    )


@pytest.mark.asyncio
async def test_handles_field_errors_caught_by_graphql(client):
    response = await client.get(url_string(query="{thrower}"))
    assert response.status == 200
    assert await response.json() == {
        "data": None,
        "errors": [
            {
                "locations": [{"column": 2, "line": 1}],
                "message": "Throws!",
                "path": ["thrower"],
            }
        ],
    }


@pytest.mark.asyncio
async def test_handles_syntax_errors_caught_by_graphql(client):
    response = await client.get(url_string(query="syntaxerror"))

    assert response.status == 400
    assert await response.json() == {
        "errors": [
            {
                "locations": [{"column": 1, "line": 1}],
                "message": "Syntax Error: Unexpected Name 'syntaxerror'.",
            },
        ],
    }


@pytest.mark.asyncio
async def test_handles_errors_caused_by_a_lack_of_query(client):
    response = await client.get("/graphql")

    assert response.status == 400
    assert await response.json() == {
        "errors": [{"message": "Must provide query string."}]
    }


@pytest.mark.asyncio
async def test_handles_batch_correctly_if_is_disabled(client):
    response = await client.post(
        "/graphql",
        data="[]",
        headers={"content-type": "application/json"},
    )

    assert response.status == 400
    assert await response.json() == {
        "errors": [
            {
                "message": "Batch GraphQL requests are not enabled.",
            }
        ]
    }


@pytest.mark.asyncio
async def test_handles_incomplete_json_bodies(client):
    response = await client.post(
        "/graphql",
        data='{"query":',
        headers={"content-type": "application/json"},
    )

    assert response.status == 400
    assert await response.json() == {
        "errors": [
            {
                "message": "POST body sent invalid JSON.",
            }
        ]
    }


@pytest.mark.asyncio
async def test_handles_plain_post_text(client):
    response = await client.post(
        url_string(variables=json.dumps({"who": "Dolly"})),
        data="query helloWho($who: String){ test(who: $who) }",
        headers={"content-type": "text/plain"},
    )
    assert response.status == 400
    assert await response.json() == {
        "errors": [{"message": "Must provide query string."}]
    }


@pytest.mark.asyncio
async def test_handles_poorly_formed_variables(client):
    response = await client.get(
        url_string(
            query="query helloWho($who: String){ test(who: $who) }", variables="who:You"
        ),
    )
    assert response.status == 400
    assert await response.json() == {
        "errors": [{"message": "Variables are invalid JSON."}]
    }


@pytest.mark.asyncio
async def test_handles_unsupported_http_methods(client):
    response = await client.put(url_string(query="{test}"))
    assert response.status == 405
    assert response.headers["Allow"] in ["GET, POST", "HEAD, GET, POST, OPTIONS"]
    assert await response.json() == {
        "errors": [
            {
                "message": "GraphQL only supports GET and POST requests.",
            }
        ]
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app()])
async def test_passes_request_into_request_context(app, client):
    response = await client.get(url_string(query="{request}", q="testing"))

    assert response.status == 200
    assert await response.json() == {
        "data": {"request": "testing"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(context={"session": "CUSTOM CONTEXT"})])
async def test_passes_custom_context_into_context(app, client):
    response = await client.get(url_string(query="{context { session request }}"))

    _json = await response.json()
    assert response.status == 200
    assert "data" in _json
    assert "session" in _json["data"]["context"]
    assert "request" in _json["data"]["context"]
    assert "CUSTOM CONTEXT" in _json["data"]["context"]["session"]
    assert "Request" in _json["data"]["context"]["request"]


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(context="CUSTOM CONTEXT")])
async def test_context_remapped_if_not_mapping(app, client):
    response = await client.get(url_string(query="{context { session request }}"))

    _json = await response.json()
    assert response.status == 200
    assert "data" in _json
    assert "session" in _json["data"]["context"]
    assert "request" in _json["data"]["context"]
    assert "CUSTOM CONTEXT" not in _json["data"]["context"]["request"]
    assert "Request" in _json["data"]["context"]["request"]


class CustomContext(dict):
    property = "A custom property"


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(context=CustomContext())])
async def test_allow_empty_custom_context(app, client):
    response = await client.get(url_string(query="{context { property request }}"))

    _json = await response.json()
    assert response.status == 200
    assert "data" in _json
    assert "request" in _json["data"]["context"]
    assert "property" in _json["data"]["context"]
    assert "A custom property" == _json["data"]["context"]["property"]
    assert "Request" in _json["data"]["context"]["request"]


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(context={"request": "test"})])
async def test_request_not_replaced(app, client):
    response = await client.get(url_string(query="{context { request }}"))

    _json = await response.json()
    assert response.status == 200
    assert _json["data"]["context"]["request"] == "test"


@pytest.mark.asyncio
async def test_post_multipart_data(client):
    query = "mutation TestMutation { writeTest { test } }"

    data = (
        "------aiohttpgraphql\r\n"
        'Content-Disposition: form-data; name="query"\r\n'
        "\r\n" + query + "\r\n"
        "------aiohttpgraphql--\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        'Content-Disposition: form-data; name="file"; filename="text1.txt";'
        " filename*=utf-8''text1.txt\r\n"
        "\r\n"
        "\r\n"
        "------aiohttpgraphql--\r\n"
    )

    response = await client.post(
        "/graphql",
        data=data,
        headers={"content-type": "multipart/form-data; boundary=----aiohttpgraphql"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"writeTest": {"test": "Hello World"}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(batch=True)])
async def test_batch_allows_post_with_json_encoding(app, client):
    response = await client.post(
        "/graphql",
        data=json.dumps([dict(id=1, query="{test}")]),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == [{"data": {"test": "Hello World"}}]


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(batch=True)])
async def test_batch_supports_post_json_query_with_json_variables(app, client):
    response = await client.post(
        "/graphql",
        data=json.dumps(
            [
                dict(
                    id=1,
                    query="query helloWho($who: String){ test(who: $who) }",
                    variables={"who": "Dolly"},
                )
            ]
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == [{"data": {"test": "Hello Dolly"}}]


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(batch=True)])
async def test_batch_allows_post_with_operation_name(app, client):
    response = await client.post(
        "/graphql",
        data=json.dumps(
            [
                dict(
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
                )
            ]
        ),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == [
        {"data": {"test": "Hello World", "shared": "Hello Everyone"}}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [create_app(schema=AsyncSchema, enable_async=True)])
async def test_async_schema(app, client):
    response = await client.get(url_string(query="{a,b,c}"))

    assert response.status == 200
    assert await response.json() == {"data": {"a": "hey", "b": "hey2", "c": "hey3"}}


@pytest.mark.asyncio
async def test_preflight_request(client):
    response = await client.options(
        "/graphql",
        headers={"Access-Control-Request-Method": "POST"},
    )

    assert response.status == 200


@pytest.mark.asyncio
async def test_preflight_incorrect_request(client):
    response = await client.options(
        "/graphql",
        headers={"Access-Control-Request-Method": "OPTIONS"},
    )

    assert response.status == 400


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "app", [create_app(execution_context_class=RepeatExecutionContext)]
)
async def test_custom_execution_context_class(client):
    response = await client.post(
        "/graphql",
        data=json.dumps(dict(query="{test}")),
        headers={"content-type": "application/json"},
    )

    assert response.status == 200
    assert await response.json() == {"data": {"test": "Hello WorldHello World"}}
