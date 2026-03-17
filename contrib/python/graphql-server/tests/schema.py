from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)


def resolve_thrower(*_args):
    raise Exception("Throws!")


def resolve_request(_obj, info):
    return info.context.get("q")


def resolve_context(_obj, info):
    return str(info.context)


QueryRootType = GraphQLObjectType(
    name="QueryRoot",
    fields={
        "thrower": GraphQLField(GraphQLNonNull(GraphQLString), resolve=resolve_thrower),
        "request": GraphQLField(GraphQLNonNull(GraphQLString), resolve=resolve_request),
        "context": GraphQLField(GraphQLNonNull(GraphQLString), resolve=resolve_context),
        "test": GraphQLField(
            type_=GraphQLString,
            args={"who": GraphQLArgument(GraphQLString)},
            resolve=lambda obj, info, who="World": "Hello %s" % who,
        ),
    },
)

MutationRootType = GraphQLObjectType(
    name="MutationRoot",
    fields={
        "writeTest": GraphQLField(type_=QueryRootType, resolve=lambda *_: QueryRootType)
    },
)

schema = GraphQLSchema(QueryRootType, MutationRootType)
invalid_schema = GraphQLSchema()
