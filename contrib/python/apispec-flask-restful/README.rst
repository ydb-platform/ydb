=====================
apispec_flask_restful
=====================

.. image:: https://badge.fury.io/py/apispec-flask-restful.svg
    :target: https://badge.fury.io/py/apispec-flask-restful

Flask-RESTful plugin.

Includes a path helper that allows you to pass a Flask-RESTful resource object to `path`.

Inspired by AndrewPashkin/apispec_restful plugin.

Install
=======

::

    pip install apispec_flask_restful

Usage
===========

Typical usage
-------------

.. code-block:: python

    from pprint import pprint

    from flask_restful import Api, Resource
    from flask import Flask
    from apispec import APISpec
    from apispec_flask_restful import RestfulPlugin

    class HelloResource(Resource):
        def get(self, hello_id):
            '''A greeting endpoint.
                   ---
                   description: get a greeting
                   responses:
                       200:
                           description: a pet to be returned
                           schema:
                               $ref: #/definitions/Pet
            '''
            pass

    app = Flask(__name__)
    api = Api(app)
    spec = APISpec(title='Spec', version='1.0', openapi_version='3.0.2', plugins=[RestfulPlugin()])

    api.add_resource(HelloResource, '/hello')

    spec.path(resource=HelloResource, api=api)
    pprint(spec.to_dict()['paths'])

    # OrderedDict([('/hello',
    #          {'get': {'description': 'get a greeting',
    #                   'responses': {200: {'description': 'a pet to be returned',
    #                                       'schema': {'$ref': None}}}}})])

Without API
-----------

Method `path` can be invoked with a resource path in a `path` parameter instead of `api` parameter:

.. code-block:: python

        spec.path(resource=HelloResource, path='/hello')

With Blueprint
--------------

Flask blueprints are supported too by passing Flask app in `app` parameter:

.. code-block:: python

        spec.path(resource=HelloResource, api=api, app=app)

