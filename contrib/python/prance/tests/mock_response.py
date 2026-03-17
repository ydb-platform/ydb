"""Utility code for tests."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import pytest


class MockResponse:
    """
    Minimally imitate a response object from the requests library.

    Implements all that's required for prance.util.url
    """

    def __init__(self, *args, **kwargs):
        self.ok = kwargs.get("is_ok", True)
        self.headers = kwargs.get("headers", {"content-type": "text/plain"})
        self.text = kwargs.get("text", "")


# The petstore.yaml file served at
# http://finkhaeuser.de/projects/prance/petstore.yaml
PETSTORE_YAML = """swagger: "2.0"
info:
  version: 1.0.0
  title: Swagger Petstore
  license:
    name: MIT
host: petstore.swagger.io
basePath: /v1
schemes:
  - http
consumes:
  - application/json
produces:
  - application/json
paths:
  /pets:
    get:
      summary: List all pets
      operationId: listPets
      tags:
        - pets
      parameters:
        - name: limit
          in: query
          description: How many items to return at one time (max 100)
          required: false
          type: integer
          format: int32
      responses:
        "200":
          description: An paged array of pets
          headers:
            x-next:
              type: string
              description: A link to the next page of responses
          schema:
            $ref: '#/definitions/Pets'
        default:
          description: unexpected error
          schema:
            $ref: '#/definitions/Error'
    post:
      summary: Create a pet
      operationId: createPets
      tags:
        - pets
      responses:
        "201":
          description: Null response
        default:
          description: unexpected error
          schema:
            $ref: '#/definitions/Error'
  /pets/{petId}:
    get:
      summary: Info for a specific pet
      operationId: showPetById
      tags:
        - pets
      parameters:
        - name: petId
          in: path
          required: true
          description: The id of the pet to retrieve
          type: string
      responses:
        "200":
          description: Expected response to a valid request
          schema:
            $ref: '#/definitions/Pets'
        default:
          description: unexpected error
          schema:
            $ref: '#/definitions/Error'
definitions:
  Pet:
    required:
      - id
      - name
    properties:
      id:
        type: integer
        format: int64
      name:
        type: string
      tag:
        type: string
  Pets:
    type: array
    items:
      $ref: '#/definitions/Pet'
  Error:
    required:
      - code
      - message
    properties:
      code:
        type: integer
        format: int32
      message:
        type: string
"""
