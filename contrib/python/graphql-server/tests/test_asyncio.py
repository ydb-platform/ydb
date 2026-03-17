import asyncio

from graphql.type.definition import GraphQLField, GraphQLNonNull, GraphQLObjectType
from graphql.type.scalars import GraphQLString
from graphql.type.schema import GraphQLSchema

from graphql_server import GraphQLParams, run_http_query

from .utils import as_dicts


def resolve_error_sync(_obj, _info):
    raise ValueError("error sync")


async def resolve_error_async(_obj, _info):
    await asyncio.sleep(0.001)
    raise ValueError("error async")


def resolve_field_sync(_obj, _info):
    return "sync"


async def resolve_field_async(_obj, info):
    await asyncio.sleep(0.001)
    return "async"


NonNullString = GraphQLNonNull(GraphQLString)

QueryRootType = GraphQLObjectType(
    name="QueryRoot",
    fields={
        "errorSync": GraphQLField(NonNullString, resolve=resolve_error_sync),
        "errorAsync": GraphQLField(NonNullString, resolve=resolve_error_async),
        "fieldSync": GraphQLField(NonNullString, resolve=resolve_field_sync),
        "fieldAsync": GraphQLField(NonNullString, resolve=resolve_field_async),
    },
)

schema = GraphQLSchema(QueryRootType)


def test_get_responses_using_asyncio_executor():
    query = "{fieldSync fieldAsync}"

    async def get_results():
        result_promises, params = run_http_query(
            schema, "get", {}, dict(query=query), run_sync=False
        )
        res = [await result for result in result_promises]
        return res, params

    results, params = asyncio.run(get_results())

    expected_results = [
        {"data": {"fieldSync": "sync", "fieldAsync": "async"}, "errors": None}
    ]

    assert as_dicts(results) == expected_results
    assert params == [GraphQLParams(query=query, variables=None, operation_name=None)]
