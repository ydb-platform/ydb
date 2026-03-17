*******
apispec
*******

.. image:: https://badge.fury.io/py/apispec.svg
    :target: http://badge.fury.io/py/apispec
    :alt: Latest version

.. image:: https://travis-ci.org/marshmallow-code/apispec.svg?branch=dev
    :target: https://travis-ci.org/marshmallow-code/apispec

.. image:: https://readthedocs.org/projects/apispec/badge/
   :target: https://apispec.readthedocs.io/
   :alt: Documentation

.. image:: https://img.shields.io/badge/marshmallow-3-blue.svg
    :target: https://marshmallow.readthedocs.io/en/latest/upgrading.html
    :alt: marshmallow 3 compatible

A pluggable API specification generator. Currently supports the `OpenAPI Specification <https://github.com/OAI/OpenAPI-Specification>`_ (f.k.a. the Swagger specification).

Features
========

- Supports OpenAPI Specification version 2 (f.k.a. the Swagger specification) with limited support for version 3.
- Framework-agnostic
- Includes plugins for `marshmallow <https://marshmallow.readthedocs.io/>`_, `Flask <http://flask.pocoo.org/>`_, `Tornado <http://www.tornadoweb.org/>`_, and `bottle <http://bottlepy.org/docs/dev/>`_.
- Utilities for parsing docstrings

Example Application
===================

.. code-block:: python

    from apispec import APISpec
    from flask import Flask, jsonify
    from marshmallow import Schema, fields

    # Create an APISpec
    spec = APISpec(
        title='Swagger Petstore',
        version='1.0.0',
        plugins=[
            'apispec.ext.flask',
            'apispec.ext.marshmallow',
        ],
    )

    # Optional marshmallow support
    class CategorySchema(Schema):
        id = fields.Int()
        name = fields.Str(required=True)

    class PetSchema(Schema):
        category = fields.Nested(CategorySchema, many=True)
        name = fields.Str()

    # Optional Flask support
    app = Flask(__name__)

    @app.route('/random')
    def random_pet():
        """A cute furry animal endpoint.
        ---
        get:
            description: Get a random pet
            responses:
                200:
                    description: A pet to be returned
                    schema: PetSchema
        """
        pet = get_random_pet()
        return jsonify(PetSchema().dump(pet).data)

    # Register entities and paths
    spec.definition('Category', schema=CategorySchema)
    spec.definition('Pet', schema=PetSchema)
    with app.test_request_context():
        spec.add_path(view=random_pet)


Generated OpenAPI Spec
----------------------

.. code-block:: python

    spec.to_dict()
    # {
    #   "info": {
    #     "title": "Swagger Petstore",
    #     "version": "1.0.0"
    #   },
    #   "swagger": "2.0",
    #   "paths": {
    #     "/random": {
    #       "get": {
    #         "description": "A cute furry animal endpoint.",
    #         "responses": {
    #           "200": {
    #             "schema": {
    #               "$ref": "#/definitions/Pet"
    #             },
    #             "description": "A pet to be returned"
    #           }
    #         },
    #       }
    #     }
    #   },
    #   "definitions": {
    #     "Pet": {
    #       "properties": {
    #         "category": {
    #           "type": "array",
    #           "items": {
    #             "$ref": "#/definitions/Category"
    #           }
    #         },
    #         "name": {
    #           "type": "string"
    #         }
    #       }
    #     },
    #     "Category": {
    #       "required": [
    #         "name"
    #       ],
    #       "properties": {
    #         "name": {
    #           "type": "string"
    #         },
    #         "id": {
    #           "type": "integer",
    #           "format": "int32"
    #         }
    #       }
    #     }
    #   },
    # }

    spec.to_yaml()
    # definitions:
    #   Pet:
    #     enum: [name, photoUrls]
    #     properties:
    #       id: {format: int64, type: integer}
    #       name: {example: doggie, type: string}
    # info: {description: 'This is a sample Petstore server.  You can find out more ', title: Swagger Petstore, version: 1.0.0}
    # parameters: {}
    # paths: {}
    # security:
    # - apiKey: []
    # swagger: '2.0'
    # tags: []


Documentation
=============

Documentation is available at http://apispec.readthedocs.io/ .

Ecosystem
=========

A list of apispec-related libraries can be found at the GitHub wiki here:

https://github.com/marshmallow-code/apispec/wiki/Ecosystem

License
=======

MIT licensed. See the bundled `LICENSE <https://github.com/marshmallow-code/apispec/blob/dev/LICENSE>`_ file for more details.
