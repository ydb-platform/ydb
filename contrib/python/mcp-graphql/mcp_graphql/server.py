import functools
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from logging import INFO, WARNING, basicConfig, getLogger
from pathlib import Path
from typing import Any, cast

from gql import Client, gql
from gql.dsl import DSLField, DSLQuery, DSLSchema, DSLType, dsl_gql
from gql.transport.aiohttp import AIOHTTPTransport
from graphql import (
    GraphQLArgument,
    GraphQLArgumentMap,
    GraphQLEnumType,
    GraphQLField,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInputType,
    GraphQLInterfaceType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLOutputType,
    GraphQLScalarType,
    parse,
    print_ast,
    print_type,
)
from graphql.language.ast import OperationDefinitionNode
from graphql.pyutils import inspect
from graphql.type import GraphQLSchema
from mcp import Resource
from mcp import types as mcp_types
from mcp.server import Server
from mcp.server.lowlevel import NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import Tool
from pydantic import AnyUrl

from mcp_graphql.types import (
    JsonSchema,
    NestedSelection,
    ProcessedNestedType,
    QueryTypeNotFoundError,
    SchemaRetrievalError,
    ServerContext,
)

# Configure logging
basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = getLogger(__name__)
# Silence INFO logs from the gql AIOHTTP transport
getLogger("gql.transport.aiohttp").setLevel(WARNING)


class UnknownGraphQLTypeError(Exception):
    """Exception raised when a GraphQL type cannot be converted to JSON Schema."""

    def __init__(self, gql_type: Any) -> None:  # noqa: ANN401
        self.gql_type = gql_type
        super().__init__(f"Unknown GraphQL type: {gql_type!s}")


@asynccontextmanager
async def server_lifespan(
    server: Server[ServerContext],  # noqa: ARG001
    api_url: str,
    auth_headers: dict[str, str],
    queries_file: Path | None = None,
    queries: str | None = None,
) -> AsyncIterator[ServerContext]:
    """Manage server startup and shutdown lifecycle."""
    # Initialize resources on startup
    transport = AIOHTTPTransport(url=api_url, headers=auth_headers)
    client = Client(transport=transport, fetch_schema_from_transport=True)
    predefined_queries: dict[str, OperationDefinitionNode] = {}
    # Determine the source of predefined queries (file > string)
    doc_src: str | None = None

    if queries_file and queries_file.exists():
        try:
            with queries_file.open(encoding="utf-8") as f:
                doc_src = f.read()
        except Exception:
            logger.exception("Error reading predefined queries file")
    elif queries:
        # If provided directly as a string, use it
        doc_src = queries

    # Parse the document if we have any source
    if doc_src:
        try:
            # Parse the .gql document and extract every named operation
            doc = parse(doc_src)

            for definition in doc.definitions:
                if not isinstance(definition, OperationDefinitionNode):
                    continue
                # Only consider operations that have a name; anonymous
                # operations cannot be called as tools.
                if definition.name is None:
                    continue
                op_name = definition.name.value
                predefined_queries[op_name] = definition
        except Exception:
            logger.exception("Error while parsing predefined queries")

    async with client as session:
        try:
            context: ServerContext = {
                "session": session,
                "dsl_schema": DSLSchema(session.client.schema or GraphQLSchema()),
                "predefined_queries": predefined_queries if predefined_queries else None,
            }
            yield context
        finally:
            # No need for manual __aexit__ call - it's handled by the async with
            pass


def _convert_scalar_to_json_schema(gql_scalar: GraphQLScalarType) -> JsonSchema:
    """Convert a GraphQLScalarType to its JSON Schema representation."""
    type_name = str(gql_scalar.name).lower()
    simple_map = {
        "string": "string",
        "int": "integer",
        "float": "number",
        "boolean": "boolean",
    }

    if type_name in simple_map:
        return {"type": simple_map[type_name]}

    if type_name in {"id", "id!", "datetime"}:
        return {"type": "string"}

    # Fallback for custom scalars (e.g., DateTime)
    return {"type": "string", "description": f"GraphQL scalar: {gql_scalar!s}"}


def convert_type_to_json_schema(  # noqa: C901
    gql_type: GraphQLInputType | GraphQLArgument,
    max_depth: int = 5,
    current_depth: int = 1,
) -> JsonSchema:
    """
    Convert GraphQL type to JSON Schema, handling complex nested types properly.
    Supports max_depth to prevent infinite recursion with circular references.
    """
    # Check max depth to prevent infinite recursion
    if current_depth > max_depth:
        return {"type": "object", "description": "Max depth reached"}

    # We will build the schema incrementally to avoid many early returns.
    schema: JsonSchema

    if isinstance(gql_type, GraphQLNonNull):
        # Non-null wrapper
        schema = convert_type_to_json_schema(gql_type.of_type, max_depth, current_depth)
    elif isinstance(gql_type, GraphQLList):
        # List wrapper
        inner_schema = convert_type_to_json_schema(gql_type.of_type, max_depth, current_depth)
        schema = {"type": "array", "items": inner_schema}

    elif isinstance(gql_type, GraphQLScalarType):
        # Scalar value
        schema = _convert_scalar_to_json_schema(gql_type)

    elif isinstance(gql_type, GraphQLEnumType):
        schema = {"enum": [value.value for value in gql_type.values.values()]}
    elif isinstance(gql_type, GraphQLObjectType):
        # Object with fields
        schema = {
            "type": "object",
            "properties": {
                field_name: convert_type_to_json_schema(field_type, max_depth, current_depth)
                for field_name, field_type in gql_type.fields.items()
            },
            "required": [
                field_name
                for field_name, field_type in gql_type.fields.items()
                if isinstance(field_type.type, GraphQLNonNull)
            ],
        }

    elif isinstance(gql_type, GraphQLField):
        # Field with arguments
        schema = {
            "type": "object",
            "properties": {
                field_name: convert_type_to_json_schema(field_type, max_depth, current_depth)
                for field_name, field_type in gql_type.args.items()
            },
            "required": [
                field_name
                for field_name, field_type in gql_type.args.items()
                if isinstance(field_type.type, GraphQLNonNull)
            ],
        }

    elif isinstance(gql_type, GraphQLArgument):
        # Argument type (possibly with description)
        schema = convert_type_to_json_schema(gql_type.type, max_depth, current_depth)
        if gql_type.description is not None:
            schema["description"] = gql_type.description
    elif isinstance(gql_type, GraphQLInputObjectType):
        schema = {
            "type": "object",
            "properties": {
                field_name: convert_type_to_json_schema(field_type, max_depth, current_depth)
                for field_name, field_type in gql_type.fields.items()
            },
            "required": [
                field_name
                for field_name, field_type in gql_type.fields.items()
                if isinstance(field_type.type, GraphQLNonNull)
            ],
        }
    elif isinstance(gql_type, GraphQLInputField):
        schema = convert_type_to_json_schema(gql_type.type, max_depth, current_depth)
    else:
        # Unknown / unsupported
        logger.error("Unknown GraphQL type: %s", gql_type.__class__.__name__)
        raise UnknownGraphQLTypeError(gql_type)

    return schema


def _process_nested_type(
    field_name: str,
    nested_type: GraphQLOutputType,
    max_depth: int,
    current_depth: int,
) -> ProcessedNestedType:
    """Process a nested type field."""
    # Unwrap wrappers to get the concrete type
    nested_type = _unwrap_wrapped_type(nested_type)

    # Only process if we actually have a GraphQLObjectType
    if isinstance(nested_type, GraphQLObjectType):
        nested_selections = build_nested_selection(
            nested_type,
            max_depth,
            current_depth + 1,
        )
        # Only append if there are valid nested selections
        if nested_selections:
            return (field_name, nested_selections)
    return (field_name, None)  # Return properly typed tuple instead of None


def _should_skip_field(field_name: str, field_value: GraphQLField) -> bool:
    """Return True if the field must be ignored when building nested selections."""
    # Skip internal or argument-expecting fields
    return field_name.startswith("__") or bool(field_value.args)


def build_nested_selection(
    field_type: GraphQLObjectType | GraphQLInterfaceType,
    max_depth: int,
    current_depth: int = 1,
) -> NestedSelection:
    """Recursively build nested selections up to the specified depth."""

    # Guard clauses for depth and unsupported types
    if current_depth > max_depth or not hasattr(field_type, "fields"):
        return []

    selections: NestedSelection = []

    for field_name, field_value in cast("dict[str, GraphQLField]", field_type.fields).items():
        # Skip helper keeps the branching cost outside this function
        if _should_skip_field(field_name, field_value):
            continue

        # Determine the underlying GraphQL type (skip NonNull / List wrappers)
        value_type = _unwrap_wrapped_type(field_value.type)

        # Scalars can be added directly
        if isinstance(value_type, GraphQLScalarType):
            selections.append((field_name, None))
            continue

        # For lists or object/interface types, recurse
        result = _process_nested_type(
            field_name,
            field_value.type,
            max_depth,
            current_depth + 1,
        )
        if result:
            selections.append(result)

    return selections


def build_selection(
    ds: DSLSchema,
    parent: DSLType,
    selections: list[tuple[str, Any]],
) -> list[DSLField]:
    result = []
    for field_name, nested_selections in selections:
        # Get the field
        field = getattr(parent, field_name)

        # Get the field type and handle wrapped types (List, NonNull)
        field_type = _unwrap_wrapped_type(field.field.type)

        # Check if this is a scalar type or an object type
        is_scalar = isinstance(field_type, GraphQLScalarType)

        if nested_selections is None and is_scalar:
            # This is a scalar field - can be selected directly
            result.append(getattr(parent, field_name))
        elif nested_selections and len(nested_selections) > 0:
            # This is a non-scalar with valid nested selections
            nested_fields = build_selection(
                ds,
                getattr(ds, cast("Any", field_type).name),
                nested_selections,
            )
            if nested_fields:
                result.append(field.select(*nested_fields))
        # Skip fields that have no valid nested selections and aren't scalars

    return result


def get_args_schema(args_map: GraphQLArgumentMap, max_depth: int = 5) -> JsonSchema:
    args_schema: JsonSchema = {"type": "object", "properties": {}, "required": []}
    for arg_name, arg in args_map.items():
        type_schema = convert_type_to_json_schema(arg.type, max_depth=max_depth, current_depth=1)
        # Remove the "required" flag which was used for tracking
        is_required = type_schema.pop("required", False)

        args_schema["properties"][arg_name] = type_schema
        args_schema["properties"][arg_name]["description"] = (
            arg.description if arg.description else f"Argument {arg_name}"
        )

        # Mark as required if non-null and no default value
        if (
            (is_required or str(arg.type).startswith("!"))
            and not arg.default_value
            and not isinstance(args_schema["required"], bool)
        ):
            args_schema["required"].append(arg_name)
    return args_schema


async def list_tools_impl(_server: Server[ServerContext], max_depth: int = 5) -> list[Tool]:
    try:
        ctx = _server.request_context
        ds: DSLSchema = ctx.lifespan_context["dsl_schema"]
    except LookupError as exc:
        logger.exception(
            "Error obtaining context",
        )
        # Configura el transporte
        transport = AIOHTTPTransport(url="http://localhost:8080/graphql")

        # Crea el cliente con fetch_schema_from_transport=True
        client = Client(transport=transport, fetch_schema_from_transport=True)
        async with client as session:
            if not session.client.schema:
                raise SchemaRetrievalError from exc
            ds = DSLSchema(session.client.schema)

    # Access context pieces
    predefined_queries: dict[str, OperationDefinitionNode] = (
        ctx.lifespan_context.get("predefined_queries") or {}
    )

    tools: list[Tool] = []

    if not ds._schema.query_type:
        raise QueryTypeNotFoundError
    # Determine which query names we should expose as tools
    if predefined_queries:
        query_names = list(predefined_queries.keys())
    else:
        query_names = list(ds._schema.query_type.fields.keys())

    logger.info("number of query names: %s", len(query_names))
    # Iterate over the selected query names and build Tool objects
    fields: dict[str, GraphQLField] = ds._schema.query_type.fields
    if not ds or not ds._schema.query_type or not fields:
        raise QueryTypeNotFoundError

    for query_definition in predefined_queries.values():
        # Skip if the query does not exist in the schema (e.g. mutation)
        query_name = query_definition.selection_set.selections[0].to_dict()["name"]["value"]
        if query_name not in fields:
            continue

        field = fields[query_name]
        dsl_field: DSLField = getattr(ds.Query, query_name)
        return_type_description = inspect(dsl_field.field.type)
        # Get the arguments schema for this field
        args_schema = get_args_schema(dsl_field.field.args, max_depth=max_depth)
        tools.append(
            Tool(
                name=query_definition.name.value,  # type: ignore[union-attr]
                description=(field.description or f"GraphQL query: {query_name}")
                + f" (Returns: {return_type_description})",
                inputSchema=args_schema,  # type: ignore[arg-type]
            ),
        )
    for query_name, field in fields.items() if not predefined_queries else []:
        dsl_field = getattr(ds.Query, query_name)
        return_type_description = inspect(dsl_field.field.type)
        # Get the arguments schema for this field
        args_schema = get_args_schema(dsl_field.field.args, max_depth=max_depth)
        tools.append(
            Tool(
                name=query_name,
                description=(field.description or f"GraphQL query: {query_name}")
                + f" (Returns: {return_type_description})",
                inputSchema=args_schema,  # type: ignore[arg-type]
            ),
        )

    return tools


async def call_tool_impl(
    _server: Server[ServerContext],
    max_depth: int,
    name: str,
    arguments: dict[str, Any],
) -> list[mcp_types.TextContent]:
    ctx = _server.request_context

    # Access context pieces
    session = ctx.lifespan_context["session"]
    predefined_queries = ctx.lifespan_context.get("predefined_queries") or {}

    # Don't use the session as a context manager, use it directly
    ds: DSLSchema = ctx.lifespan_context["dsl_schema"]
    if not ds._schema.query_type:
        raise QueryTypeNotFoundError
    fields: dict[str, GraphQLField] = ds._schema.query_type.fields

    # ---------------------------------------------------------------------
    # 1. If the query comes from the predefined queries file, execute it as-is
    # ---------------------------------------------------------------------
    if name in predefined_queries:
        query_src = predefined_queries[name]
        try:
            result = await session.execute(
                gql(print_ast(query_src)),
                variable_values=arguments or None,
            )
            return [mcp_types.TextContent(type="text", text=json.dumps(result))]
        except Exception as exc:
            logger.exception("Error executing predefined query %s", name)
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"Error executing query {name}: {exc}",
                ),
            ]

    # ---------------------------------------------------------------------
    # 2. Fallback to dynamic query generation from the schema
    # ---------------------------------------------------------------------

    # If the server was started with predefined queries, reject any other
    if predefined_queries:
        return [
            mcp_types.TextContent(
                type="text",
                text=f"The query '{name}' is not among the allowed queries.",
            ),
        ]

    if _query_name := next((_query_name for _query_name in fields if _query_name == name), None):
        attr: DSLField = getattr(ds.Query, _query_name)

        # Unwrap the type (NonNull, List) to get to the actual type name
        field_type = attr.field.type
        # Unwrap until we hit a type that exposes a ``name`` attribute
        while not hasattr(field_type, "name") and hasattr(field_type, "of_type"):
            # Access dynamically to appease static type checkers
            field_type = field_type.of_type

        # Ensure we end up with the innermost, unwrapped type
        field_type = _unwrap_wrapped_type(field_type)

        # Now we should have the actual type with a name
        if not hasattr(field_type, "name"):
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"Error: Could not determine the return type for {name}",
                ),
            ]

        return_type: DSLType = getattr(ds, cast("Any", field_type).name)

        # Build the query with nested selections
        selections = build_nested_selection(return_type._type, max_depth)

        # Build the actual query
        query_selections = build_selection(ds, return_type, selections)
        query = dsl_gql(DSLQuery(attr(**arguments).select(*query_selections)))

        #     # Execute the query
        result = await session.execute(query)
        return [mcp_types.TextContent(type="text", text=json.dumps(result))]

    # Error case - tool not found
    return [mcp_types.TextContent(type="text", text="Tool not found")]


async def serve(
    api_url: str,
    auth_headers: dict[str, str] | None,
    queries_file: Path | None = None,
    queries: str | None = None,
    max_depth: int = 5,
) -> None:
    server = Server[ServerContext](
        "mcp-graphql",
        lifespan=partial(
            server_lifespan,
            api_url=api_url,
            auth_headers=auth_headers or {},
            queries_file=queries_file,
            queries=queries,
        ),
    )

    server.list_tools()(functools.partial(list_tools_impl, server, max_depth))
    server.call_tool()(functools.partial(call_tool_impl, server, max_depth))

    @server.list_resources()  # type: ignore[misc]
    async def list_resources_impl() -> list[Resource]:
        resources = []
        ctx = server.request_context
        ds: DSLSchema = ctx.lifespan_context["dsl_schema"]
        for type_name, graphql_type in ds._schema.type_map.items():
            resources.append(
                Resource(
                    uri=AnyUrl(f"scheme://types/{type_name}"),
                    name=type_name,
                    description=graphql_type.description,
                ),
            )
        return resources

    @server.read_resource()  # type: ignore[misc]
    async def read_resource_impl(uri: AnyUrl) -> Resource:
        ctx = server.request_context
        ds: DSLSchema = ctx.lifespan_context["dsl_schema"]
        type_name = uri.path.split("/")[-1] if uri.path else ""
        if not type_name or type_name not in ds._schema.type_map:
            msg = f"Invalid type name: {type_name}"
            raise ValueError(msg)
        return Resource(
            uri=uri,
            name=type_name,
            description=print_type(ds._schema.type_map[type_name]),
        )

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mcp-graphql",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def _unwrap_wrapped_type(gql_type: Any) -> Any:  # noqa: ANN401
    """Return the innermost GraphQL type.

    GraphQL exposes wrapper types (``GraphQLNonNull``/``GraphQLList``) that add
    an ``of_type`` attribute pointing at the underlying type.  For code that
    needs the concrete type (e.g. to check whether it is a scalar/object) we
    repeatedly follow that attribute until we reach a type that is not itself a
    wrapper.  The parameter is typed as *Any* so static analysers do not shout
    about the dynamic attribute access.
    """

    while hasattr(gql_type, "of_type"):
        gql_type = gql_type.of_type
    return gql_type
