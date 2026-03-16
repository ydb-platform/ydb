from flask import Flask

from graphql_server.flask import GraphQLView
from .schema import Schema


def create_app(path="/graphql", **kwargs):
    server = Flask(__name__)
    server.debug = True
    server.add_url_rule(
        path, view_func=GraphQLView.as_view("graphql", schema=Schema, **kwargs)
    )
    return server


if __name__ == "__main__":
    app = create_app(graphiql=True)
    app.run()
