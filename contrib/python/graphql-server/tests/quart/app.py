from quart import Quart

from graphql_server.quart import GraphQLView
from tests.quart.schema import Schema


def create_app(path="/graphql", schema=Schema, **kwargs):
    server = Quart(__name__)
    server.debug = True
    server.add_url_rule(
        path, view_func=GraphQLView.as_view("graphql", schema=schema, **kwargs)
    )
    return server


if __name__ == "__main__":
    app = create_app(graphiql=True)
    app.run()
