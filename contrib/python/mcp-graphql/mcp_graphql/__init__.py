import asyncio
import json
from pathlib import Path
from typing import Any

import click

from .server import serve


class JsonParamType(click.ParamType):
    name = "json"

    def convert(
        self,
        value: str | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> dict[str, Any] | None:
        if value is None:
            return None
        try:
            return json.loads(value)  # type: ignore[no-any-return]
        except json.JSONDecodeError:
            self.fail(f"'{value}' is not a valid JSON string", param, ctx)


@click.command(help="MCP Graphql Server - Graphql server for MCP")
@click.option("--api-url", type=str, required=True, help="URL of the GraphQL API")
@click.option(
    "--auth-token",
    type=str,
    help="Authentication token (optional)",
    envvar="MCP_AUTH_TOKEN",
)
@click.option(
    "--auth-type",
    type=str,
    default="Bearer",
    help="Authentication type (Bearer, Basic, etc.)",
)
@click.option(
    "--auth-headers",
    type=JsonParamType(),
    help=(
        'Custom authentication headers as JSON string (e.g. \'{"Authorization": "Bearer token", "X-API-Key": "key"}\')'  # noqa: E501
    ),
)
@click.option(
    "--queries-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a .gql file with predefined GraphQL queries (optional)",
)
@click.option(
    "--queries",
    type=str,
    help="Predefined GraphQL queries as a string (optional)",
)
@click.option(
    "--max-depth",
    type=int,
    default=5,
    help="Maximum depth of the GraphQL query to generate (optional)",
)
def main(  # noqa: PLR0913
    api_url: str,
    auth_token: str | None,
    auth_type: str,
    auth_headers: dict[str, Any] | None,
    queries_file: Path | None,
    queries: str | None,
    max_depth: int,
) -> None:
    """MCP Graphql Server - Graphql server for MCP"""

    # Create auth headers
    auth_headers_dict = {}

    # First try to use auth_headers if provided
    if auth_headers:
        auth_headers_dict = auth_headers
    # Otherwise use auth_token and auth_type if provided
    elif auth_token:
        auth_headers_dict["Authorization"] = f"{auth_type} {auth_token}"

    asyncio.run(
        serve(
            api_url,
            auth_headers_dict,
            queries_file=queries_file,
            queries=queries,
            max_depth=max_depth,
        ),
    )


if __name__ == "__main__":
    main()

# Export the main function
__all__ = ["main"]
