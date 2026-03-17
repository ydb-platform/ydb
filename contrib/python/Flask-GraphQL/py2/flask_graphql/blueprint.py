import warnings

from flask import Blueprint

from .graphqlview import GraphQLView


class GraphQL(object):
    def __init__(self, app, schema, **options):
        self.app = app
        warnings.warn('GraphQL Blueprint is now deprecated, please use GraphQLView directly')
        self.blueprint = Blueprint('graphql', __name__,
                                   template_folder='templates')

        app.add_url_rule('/graphql', view_func=GraphQLView.as_view('graphql', schema=schema, **options))

        self.app.register_blueprint(self.blueprint)
