Flask-GraphQL
=============

|Build Status| |Coverage Status| |PyPI version|

Adds GraphQL support to your Flask application.

Usage
-----

Just use the ``GraphQLView`` view from ``flask_graphql``

.. code:: python

    from flask_graphql import GraphQLView

    app.add_url_rule('/graphql', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))

    # Optional, for adding batch query support (used in Apollo-Client)
    app.add_url_rule('/graphql/batch', view_func=GraphQLView.as_view('graphql', schema=schema, batch=True))

This will add ``/graphql`` and ``/graphiql`` endpoints to your app.

Supported options
~~~~~~~~~~~~~~~~~

-  ``schema``: The ``GraphQLSchema`` object that you want the view to
   execute when it gets a valid request.
-  ``context``: A value to pass as the ``context`` to the ``graphql()``
   function.
-  ``root_value``: The ``root_value`` you want to provide to
   ``executor.execute``.
-  ``pretty``: Whether or not you want the response to be pretty printed
   JSON.
-  ``executor``: The ``Executor`` that you want to use to execute
   queries.
-  ``graphiql``: If ``True``, may present
   `GraphiQL <https://github.com/graphql/graphiql>`__ when loaded
   directly from a browser (a useful tool for debugging and
   exploration).
-  ``graphiql_template``: Inject a Jinja template string to customize
   GraphiQL.
-  ``batch``: Set the GraphQL view as batch (for using in
   `Apollo-Client <http://dev.apollodata.com/core/network.html#query-batching>`__
   or
   `ReactRelayNetworkLayer <https://github.com/nodkz/react-relay-network-layer>`__)

You can also subclass ``GraphQLView`` and overwrite
``get_root_value(self, request)`` to have a dynamic root value per
request.

.. code:: python

    class UserRootValue(GraphQLView):
        def get_root_value(self, request):
            return request.user

.. |Build Status| image:: https://travis-ci.org/graphql-python/flask-graphql.svg?branch=master
   :target: https://travis-ci.org/graphql-python/flask-graphql
.. |Coverage Status| image:: https://coveralls.io/repos/graphql-python/flask-graphql/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/graphql-python/flask-graphql?branch=master
.. |PyPI version| image:: https://badge.fury.io/py/flask-graphql.svg
   :target: https://badge.fury.io/py/flask-graphql
