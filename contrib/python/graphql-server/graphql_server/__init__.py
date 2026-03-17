"""
GraphQL-Server
===================

GraphQL-Server is a base library that serves as a helper
for building GraphQL servers or integrations into existing web frameworks using
[GraphQL-Core](https://github.com/graphql-python/graphql-core).
"""
import json
from collections import namedtuple
from collections.abc import MutableMapping
from typing import Any, Callable, Collection, Dict, List, Optional, Type, Union, cast

from graphql.error import GraphQLError
from graphql.execution import ExecutionResult, execute
from graphql.language import OperationType, parse
from graphql.pyutils import AwaitableOrValue
from graphql.type import GraphQLSchema, validate_schema
from graphql.utilities import get_operation_ast
from graphql.validation import ASTValidationRule, validate

from .error import HttpQueryError
from .version import version, version_info

# The GraphQL-Server 3 version info.

__version__ = version
__version_info__ = version_info

__all__ = [
    "version",
    "version_info",
    "run_http_query",
    "encode_execution_results",
    "load_json_body",
    "json_encode",
    "json_encode_pretty",
    "HttpQueryError",
    "GraphQLParams",
    "GraphQLResponse",
    "ServerResponse",
    "format_execution_result",
    "format_error_default",
]


# The public data structures

GraphQLParams = namedtuple("GraphQLParams", "query variables operation_name")
GraphQLResponse = namedtuple("GraphQLResponse", "results params")
ServerResponse = namedtuple("ServerResponse", "body status_code")


# The public helper functions


def format_error_default(error: GraphQLError) -> Dict:
    """The default function for converting GraphQLError to a dictionary."""
    return cast(Dict, error.formatted)


def run_http_query(
    schema: GraphQLSchema,
    request_method: str,
    data: Union[Dict, List[Dict]],
    query_data: Optional[Dict] = None,
    batch_enabled: bool = False,
    catch: bool = False,
    run_sync: bool = True,
    **execute_options,
) -> GraphQLResponse:
    """Execute GraphQL coming from an HTTP query against a given schema.

    You need to pass the schema (that is supposed to be already validated),
    the request_method (that must be either "get" or "post"),
    the data from the HTTP request body, and the data from the query string.
    By default, only one parameter set is expected, but if you set batch_enabled,
    you can pass data that contains a list of parameter sets to run multiple
    queries as a batch execution using a single HTTP request. You can specify
    whether results returning HTTPQueryErrors should be caught and skipped.
    All other keyword arguments are passed on to the GraphQL-Core function for
    executing GraphQL queries.

    Returns a ServerResults tuple with the list of ExecutionResults as first item
    and the list of parameters that have been used for execution as second item.
    """
    if not isinstance(schema, GraphQLSchema):
        raise TypeError(f"Expected a GraphQL schema, but received {schema!r}.")
    if request_method not in ("get", "post"):
        raise HttpQueryError(
            405,
            "GraphQL only supports GET and POST requests.",
            headers={"Allow": "GET, POST"},
        )
    if catch:
        catch_exc: Union[Type[HttpQueryError], Type[_NoException]] = HttpQueryError
    else:
        catch_exc = _NoException
    is_batch = isinstance(data, list)

    is_get_request = request_method == "get"
    allow_only_query = is_get_request

    if not is_batch:
        if not isinstance(data, (dict, MutableMapping)):
            raise HttpQueryError(
                400, f"GraphQL params should be a dict. Received {data!r}."
            )
        data = [data]
    elif not batch_enabled:
        raise HttpQueryError(400, "Batch GraphQL requests are not enabled.")

    if not data:
        raise HttpQueryError(400, "Received an empty list in the batch request.")

    extra_data: Dict[str, Any] = {}
    # If is a batch request, we don't consume the data from the query
    if not is_batch:
        extra_data = query_data or {}

    all_params: List[GraphQLParams] = [
        get_graphql_params(entry, extra_data) for entry in data
    ]

    results: List[Optional[AwaitableOrValue[ExecutionResult]]] = [
        get_response(
            schema, params, catch_exc, allow_only_query, run_sync, **execute_options
        )
        for params in all_params
    ]
    return GraphQLResponse(results, all_params)


def json_encode(data: Union[Dict, List], pretty: bool = False) -> str:
    """Serialize the given data(a dictionary or a list) using JSON.

    The given data (a dictionary or a list) will be serialized using JSON
    and returned as a string that will be nicely formatted if you set pretty=True.
    """
    if not pretty:
        return json.dumps(data, separators=(",", ":"))
    return json.dumps(data, indent=2, separators=(",", ": "))


def json_encode_pretty(data: Union[Dict, List]) -> str:
    return json_encode(data, True)


def encode_execution_results(
    execution_results: List[Optional[ExecutionResult]],
    format_error: Callable[[GraphQLError], Dict] = format_error_default,
    is_batch: bool = False,
    encode: Callable[[Dict], Any] = json_encode,
) -> ServerResponse:
    """Serialize the ExecutionResults.

    This function takes the ExecutionResults that are returned by run_http_query()
    and serializes them using JSON to produce an HTTP response.
    If you set is_batch=True, then all ExecutionResults will be returned, otherwise only
    the first one will be used. You can also pass a custom function that formats the
    errors in the ExecutionResults, expecting a dictionary as result and another custom
    function that is used to serialize the output.

    Returns a ServerResponse tuple with the serialized response as the first item and
    a status code of 200 or 400 in case any result was invalid as the second item.
    """
    results = [
        format_execution_result(execution_result, format_error)
        for execution_result in execution_results
    ]
    result, status_codes = zip(*results)
    status_code = max(status_codes)

    if not is_batch:
        result = result[0]

    return ServerResponse(encode(result), status_code)


def load_json_body(data):
    # type: (str) -> Union[Dict, List]
    """Load the request body as a dictionary or a list.

    The body must be passed in a string and will be deserialized from JSON,
    raising an HttpQueryError in case of invalid JSON.
    """
    try:
        return json.loads(data)
    except Exception:
        raise HttpQueryError(400, "POST body sent invalid JSON.")


# Some more private helpers

FormattedResult = namedtuple("FormattedResult", "result status_code")


class _NoException(Exception):
    """Private exception used when we don't want to catch any real exception."""


def get_graphql_params(data: Dict, query_data: Dict) -> GraphQLParams:
    """Fetch GraphQL query, variables and operation name parameters from given data.

    You need to pass both the data from the HTTP request body and the HTTP query string.
    Params from the request body will take precedence over those from the query string.

    You will get a RequestParams tuple with these parameters in return.
    """
    query = data.get("query") or query_data.get("query")
    variables = data.get("variables") or query_data.get("variables")
    # document_id = data.get('documentId')
    operation_name = data.get("operationName") or query_data.get("operationName")

    return GraphQLParams(query, load_json_variables(variables), operation_name)


def load_json_variables(variables: Optional[Union[str, Dict]]) -> Optional[Dict]:
    """Return the given GraphQL variables as a dictionary.

    The function returns the given GraphQL variables, making sure they are
    deserialized from JSON to a dictionary first if necessary. In case of
    invalid JSON input, an HttpQueryError will be raised.
    """
    if variables and isinstance(variables, str):
        try:
            return json.loads(variables)
        except Exception:
            raise HttpQueryError(400, "Variables are invalid JSON.")
    return variables  # type: ignore


def assume_not_awaitable(_value: Any) -> bool:
    """Replacement for isawaitable if everything is assumed to be synchronous."""
    return False


def get_response(
    schema: GraphQLSchema,
    params: GraphQLParams,
    catch_exc: Type[BaseException],
    allow_only_query: bool = False,
    run_sync: bool = True,
    validation_rules: Optional[Collection[Type[ASTValidationRule]]] = None,
    max_errors: Optional[int] = None,
    **kwargs,
) -> Optional[AwaitableOrValue[ExecutionResult]]:
    """Get an individual execution result as response, with option to catch errors.

    This will validate the schema (if the schema is used for the first time),
    parse the query, check if this is a query if allow_only_query is set to True,
    validate the query (optionally with additional validation rules and limiting
    the number of errors), execute the request (asynchronously if run_sync is not
    set to True), and return the ExecutionResult. You can also catch all errors that
    belong to an exception class specified by catch_exc.
    """
    # noinspection PyBroadException
    try:
        if not params.query:
            raise HttpQueryError(400, "Must provide query string.")

        # Sanity check query
        if not isinstance(params.query, str):
            raise HttpQueryError(400, "Unexpected query type.")

        schema_validation_errors = validate_schema(schema)
        if schema_validation_errors:
            return ExecutionResult(data=None, errors=schema_validation_errors)

        try:
            document = parse(params.query)
        except GraphQLError as e:
            return ExecutionResult(data=None, errors=[e])
        except Exception as e:
            e = GraphQLError(str(e), original_error=e)
            return ExecutionResult(data=None, errors=[e])

        if allow_only_query:
            operation_ast = get_operation_ast(document, params.operation_name)
            if operation_ast:
                operation = operation_ast.operation.value
                if operation != OperationType.QUERY.value:
                    raise HttpQueryError(
                        405,
                        f"Can only perform a {operation} operation"
                        " from a POST request.",
                        headers={"Allow": "POST"},
                    )

        validation_errors = validate(
            schema, document, rules=validation_rules, max_errors=max_errors
        )
        if validation_errors:
            return ExecutionResult(data=None, errors=validation_errors)

        execution_result = execute(
            schema,
            document,
            variable_values=params.variables,
            operation_name=params.operation_name,
            is_awaitable=assume_not_awaitable if run_sync else None,
            **kwargs,
        )

    except catch_exc:
        return None

    return execution_result


def format_execution_result(
    execution_result: Optional[ExecutionResult],
    format_error: Optional[Callable[[GraphQLError], Dict]] = format_error_default,
) -> FormattedResult:
    """Format an execution result into a GraphQLResponse.

    This converts the given execution result into a FormattedResult that contains
    the ExecutionResult converted to a dictionary and an appropriate status code.
    """
    status_code = 200
    response: Optional[Dict[str, Any]] = None

    if execution_result:
        if execution_result.errors:
            fe = [format_error(e) for e in execution_result.errors]  # type: ignore
            response = {"errors": fe}

            if execution_result.errors and any(
                not getattr(e, "path", None) for e in execution_result.errors
            ):
                status_code = 400
            else:
                response["data"] = execution_result.data
        else:
            response = {"data": execution_result.data}

    return FormattedResult(response, status_code)


def _check_jinja(jinja_env: Any) -> None:
    try:
        from jinja2 import Environment
    except ImportError:  # pragma: no cover
        raise RuntimeError(
            "Attempt to set 'jinja_env' to a value other than None while Jinja2 is not installed.\n"
            "Please install Jinja2 to render GraphiQL with Jinja2.\n"
            "Otherwise set 'jinja_env' to None to use the simple regex renderer."
        )

    if not isinstance(jinja_env, Environment):  # pragma: no cover
        raise TypeError("'jinja_env' has to be of type jinja2.Environment.")
