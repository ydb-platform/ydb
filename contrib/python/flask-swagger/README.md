# flask-swagger
A Swagger 2.0 spec extractor for Flask

Install:

```shell
pip install flask-swagger
```
Flask-swagger provides a method (swagger) that inspects the Flask app for endpoints that contain YAML docstrings with Swagger 2.0 [Operation](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#operation-object) objects.

```python
class UserAPI(MethodView):

    def post(self):
        """
        Create a new user
        ---
        tags:
          - users
        definitions:
          - schema:
              id: Group
              properties:
                name:
                 type: string
                 description: the group's name
        parameters:
          - in: body
            name: body
            schema:
              id: User
              required:
                - email
                - name
              properties:
                email:
                  type: string
                  description: email for user
                name:
                  type: string
                  description: name for user
                address:
                  description: address for user
                  schema:
                    id: Address
                    properties:
                      street:
                        type: string
                      state:
                        type: string
                      country:
                        type: string
                      postalcode:
                        type: string
                groups:
                  type: array
                  description: list of groups
                  items:
                    $ref: "#/definitions/Group"
        responses:
          201:
            description: User created
        """
        return {}
```
Flask-swagger supports docstrings in methods of MethodView classes (ala [Flask-RESTful](https://github.com/flask-restful/flask-restful)) and regular Flask view functions.

Following YAML conventions, flask-swagger searches for `---`, everything preceding is provided as `summary` (first line) and `description` (following lines) for the endpoint while everything after is parsed as a swagger [Operation](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#operation-object) object.

In order to support inline definition of [Schema ](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#schemaObject) objects in [Parameter](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#parameterObject)  and [Response](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#responsesObject) objects, flask-swagger veers a little off from the standard. We require an `id` field for the inline Schema which is then used to correctly place the [Schema](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#schemaObject) object in the [Definitions](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#definitionsObject) object.


[Schema ](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#schemaObject) objects can be defined in a definitions section within the docstrings (see group object above) or within responses or parameters (see user object above). We also support schema objects nested within the properties of other [Schema ](https://github.com/swagger-api/swagger-spec/blob/master/versions/2.0.md#schemaObject) objects. An example is shown above with the address property of User.

If you don't like to put YAML on docstrings you can put the same content in an external file.

#### file.yml
```yaml
Create a new user
---
tags:
  - users
definitions:
  - schema:
      id: Group
      properties:
        name:
         type: string
         description: the group's name
parameters:
  - in: body
    name: body
    schema:
      id: User
      required:
        - email
        - name
      properties:
        email:
          type: string
          description: email for user
        name:
          type: string
          description: name for user
        address:
          description: address for user
          schema:
            id: Address
            properties:
              street:
                type: string
              state:
                type: string
              country:
                type: string
              postalcode:
                type: string
        groups:
          type: array
          description: list of groups
          items:
            $ref: "#/definitions/Group"
responses:
  201:
    description: User created
```

and point to it in your docstring.

```python
class UserAPI(MethodView):

    def post(self):
        """
        Create a new user

        blah blah

        swagger_from_file: path/to/file.yml

        blah blah
        """
        return {}
```

Note that you can replace `swagger_from_file` by another keyword. Supply your chosen keyword as an argument to swagger. 


To expose your Swagger specification to the world you provide a Flask route that does something along these lines

```python
from flask import Flask, jsonify
from flask_swagger import swagger

app = Flask(__name__)

@app.route("/spec")
def spec():
    return jsonify(swagger(app))
```

Note that the Swagger specification returned by `swagger(app)` is as minimal as it can be. It's your job to override and add to the specification as you see fit.

```python
@app.route("/spec")
def spec():
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "My API"
    return jsonify(swag)
```


[Swagger-UI](https://github.com/swagger-api/swagger-ui)

Swagger-UI is the reason we embarked on this mission to begin with, flask-swagger does not however include Swagger-UI. Simply follow the awesome documentation over at https://github.com/swagger-api/swagger-ui and point your [swaggerUi.url](https://github.com/swagger-api/swagger-ui#swaggerui) to your new flask-swagger endpoint and enjoy.

## flaskswagger Command
This package now comes with a very simple command line interface: flaskswagger. This command can be used to build and update swagger specs for your flask apps from the command line or at build time.

```shell
flaskswagger -h
```

```
usage: flaskswagger [-h] [--template TEMPLATE] [--out-dir OUT_DIR]
                    [--definitions DEFINITIONS] [--host HOST]
                    [--base-path BASE_PATH] [--version VERSION]
                    app

positional arguments:
  app                   the flask app to swaggerify

optional arguments:
  -h, --help            show this help message and exit
  --template TEMPLATE   template spec to start with, before any other options
                        or processing
  --out-dir OUT_DIR     the directory to output to
  --definitions DEFINITIONS
                        json definitions file
  --host HOST
  --base-path BASE_PATH
  --version VERSION     Specify a spec version

```

For example, this can be used to build a swagger spec which can be served from your static directory. In the example below, we use the manually created swagger.json.manual as a template, and output to the `static/` directory.

```shell
flaskswagger server:app --template static/swagger.json.manual --out-dir static/
```
Also, you can ask flaskswagger to add host and basePath to your swagger spec:  

```shell
flaskswagger server:app --host localhost:5000 --base-path /v1
```

Acknowledgements

Flask-swagger builds on ideas and code from [flask-sillywalk](https://github.com/hobbeswalsh/flask-sillywalk) and [flask-restful-swagger](https://github.com/rantav/flask-restful-swagger)

Notable forks

[Flasgger](https://github.com/rochacbruno/flasgger)
