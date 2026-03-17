import asyncio

from graphql.type.definition import (
    GraphQLArgument,
    GraphQLField,
    GraphQLNonNull,
    GraphQLObjectType,
)
from graphql.type.scalars import GraphQLString
from graphql.type.schema import GraphQLSchema


def resolve_raises(*_):
    raise Exception("Throws!")


# Sync schema
QueryRootType = GraphQLObjectType(
    name="QueryRoot",
    fields={
        "thrower": GraphQLField(GraphQLNonNull(GraphQLString), resolve=resolve_raises),
        "request": GraphQLField(
            GraphQLNonNull(GraphQLString),
            resolve=lambda obj, info: info.context["request"].args.get("q"),
        ),
        "context": GraphQLField(
            GraphQLObjectType(
                name="context",
                fields={
                    "session": GraphQLField(GraphQLString),
                    "request": GraphQLField(
                        GraphQLNonNull(GraphQLString),
                        resolve=lambda obj, info: info.context["request"],
                    ),
                    "property": GraphQLField(
                        GraphQLString, resolve=lambda obj, info: info.context.property
                    ),
                },
            ),
            resolve=lambda obj, info: info.context,
        ),
        "test": GraphQLField(
            type_=GraphQLString,
            args={"who": GraphQLArgument(GraphQLString)},
            resolve=lambda obj, info, who=None: "Hello %s" % (who or "World"),
        ),
    },
)

MutationRootType = GraphQLObjectType(
    name="MutationRoot",
    fields={
        "writeTest": GraphQLField(type_=QueryRootType, resolve=lambda *_: QueryRootType)
    },
)

Schema = GraphQLSchema(QueryRootType, MutationRootType)


# Schema with async methods
async def resolver_field_async_1(_obj, info):
    await asyncio.sleep(0.001)
    return "hey"


async def resolver_field_async_2(_obj, info):
    await asyncio.sleep(0.003)
    return "hey2"


def resolver_field_sync(_obj, info):
    return "hey3"


AsyncQueryType = GraphQLObjectType(
    name="AsyncQueryType",
    fields={
        "a": GraphQLField(GraphQLString, resolve=resolver_field_async_1),
        "b": GraphQLField(GraphQLString, resolve=resolver_field_async_2),
        "c": GraphQLField(GraphQLString, resolve=resolver_field_sync),
    },
)


def resolver_field_sync_1(_obj, info):
    return "synced_one"


def resolver_field_sync_2(_obj, info):
    return "synced_two"


SyncQueryType = GraphQLObjectType(
    "SyncQueryType",
    {
        "a": GraphQLField(GraphQLString, resolve=resolver_field_sync_1),
        "b": GraphQLField(GraphQLString, resolve=resolver_field_sync_2),
    },
)

AsyncSchema = GraphQLSchema(AsyncQueryType)
SyncSchema = GraphQLSchema(SyncQueryType)
