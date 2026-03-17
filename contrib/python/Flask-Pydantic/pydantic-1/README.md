# Flask-Pydantic

[![Actions Status](https://github.com/bauerji/flask_pydantic/workflows/Python%20package/badge.svg?branch=master)](https://github.com/bauerji/flask_pydantic/actions)
[![PyPI](https://img.shields.io/pypi/v/Flask-Pydantic?color=g)](https://pypi.org/project/Flask-Pydantic/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/bauerji/flask_pydantic.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/bauerji/flask_pydantic/context:python)
[![License](https://img.shields.io/badge/license-MIT-purple)](https://github.com/bauerji/flask_pydantic/blob/master/LICENSE)
[![Code style](https://img.shields.io/badge/code%20style-black-black)](https://github.com/psf/black)

Flask extension for integration of the awesome [pydantic package](https://github.com/samuelcolvin/pydantic) with [Flask](https://palletsprojects.com/p/flask/).

## Installation

`python3 -m pip install Flask-Pydantic`

## Basics

`validate` decorator validates query and body request parameters and makes them accessible two ways:

1. [Using `validate` arguments, via flask's `request` variable](#basic-example)

| **parameter type** | **`request` attribute name** |
| :----------------: | :--------------------------: |
|       query        |        `query_params`        |
|        body        |        `body_params`         |

2. [Using the decorated function argument parameters type hints](#using-the-decorated-function-kwargs)

---

- Success response status code can be modified via `on_success_status` parameter of `validate` decorator.
- `response_many` parameter set to `True` enables serialization of multiple models (route function should therefore return iterable of models).
- `request_body_many` parameter set to `False` analogically enables serialization of multiple models inside of the root level of request body. If the request body doesn't contain an array of objects `400` response is returned,
- If validation fails, `400` response is returned with failure explanation.

For more details see in-code docstring or example app.

## Usage

### Example 1: Query parameters only

Simply use `validate` decorator on route function.

:exclamation: Be aware that `@app.route` decorator must precede `@validate` (i. e. `@validate` must be closer to the function declaration).

```python
from typing import Optional
from flask import Flask, request
from pydantic import BaseModel

from flask_pydantic import validate

app = Flask("flask_pydantic_app")

class QueryModel(BaseModel):
  age: int

class ResponseModel(BaseModel):
  id: int
  age: int
  name: str
  nickname: Optional[str]

# Example 1: query parameters only
@app.route("/", methods=["GET"])
@validate()
def get(query:QueryModel):
  age = query.age
  return ResponseModel(
    age=age,
    id=0, name="abc", nickname="123"
    )
```

<a href="blob/master/example_app/example.py">
  See the full example app here
</a>


- `age` query parameter is a required `int`
  - `curl --location --request GET 'http://127.0.0.1:5000/'`
  - if none is provided the response contains:
    ```json
    {
      "validation_error": {
        "query_params": [
          {
            "loc": ["age"],
            "msg": "field required",
            "type": "value_error.missing"
          }
        ]
      }
    }
    ```
  - for incompatible type (e. g. string `/?age=not_a_number`)
  - `curl --location --request GET 'http://127.0.0.1:5000/?age=abc'`
    ```json
    {
      "validation_error": {
        "query_params": [
          {
            "loc": ["age"],
            "msg": "value is not a valid integer",
            "type": "type_error.integer"
          }
        ]
      }
    }
    ```
- likewise for body parameters
- example call with valid parameters:
  `curl --location --request GET 'http://127.0.0.1:5000/?age=20'`  

-> `{"id": 0, "age": 20, "name": "abc", "nickname": "123"}`



### Example 2: Request body only

```python
class RequestBodyModel(BaseModel):
  name: str
  nickname: Optional[str]

# Example2: request body only
@app.route("/", methods=["POST"])
@validate()
def post(body:RequestBodyModel): 
  name = body.name
  nickname = body.nickname
  return ResponseModel(
    name=name, nickname=nickname,id=0, age=1000
    )
```

<a href="blob/master/example_app/example.py">
  See the full example app here
</a>

### Example 3: BOTH query paramaters and request body

```python
# Example 3: both query paramters and request body
@app.route("/both", methods=["POST"])
@validate()
def get_and_post(body:RequestBodyModel,query:QueryModel):
  name = body.name # From request body
  nickname = body.nickname # From request body
  age = query.age # from query parameters
  return ResponseModel(
    age=age, name=name, nickname=nickname,
    id=0
  )
```

<a href="blob/master/example_app/example.py">
  See the full example app here
</a>




### Modify response status code

The default success status code is `200`. It can be modified in two ways

- in return statement

```python
# necessary imports, app and models definition
...

@app.route("/", methods=["POST"])
@validate(body=BodyModel, query=QueryModel)
def post():
    return ResponseModel(
            id=id_,
            age=request.query_params.age,
            name=request.body_params.name,
            nickname=request.body_params.nickname,
        ), 201
```

- in `validate` decorator

```python
@app.route("/", methods=["POST"])
@validate(body=BodyModel, query=QueryModel, on_success_status=201)
def post():
    ...
```

Status code in case of validation error can be modified using `FLASK_PYDANTIC_VALIDATION_ERROR_STATUS_CODE` flask configuration variable.

### Using the decorated function `kwargs`

Instead of passing `body` and `query` to `validate`, it is possible to directly
defined them by using type hinting in the decorated function.

```python
# necessary imports, app and models definition
...

@app.route("/", methods=["POST"])
@validate()
def post(body: BodyModel, query: QueryModel):
    return ResponseModel(
            id=id_,
            age=query.age,
            name=body.name,
            nickname=body.nickname,
        )
```

This way, the parsed data will be directly available in `body` and `query`.
Furthermore, your IDE will be able to correctly type them.

### Model aliases

Pydantic's [alias feature](https://pydantic-docs.helpmanual.io/usage/model_config/#alias-generator) is natively supported for query and body models.
To use aliases in response modify response model
```python
def modify_key(text: str) -> str:
    # do whatever you want with model keys
    return text


class MyModel(BaseModel):
    ...
    class Config:
        alias_generator = modify_key
        allow_population_by_field_name = True

```

and set `response_by_alias=True` in `validate` decorator
```
@app.route(...)
@validate(response_by_alias=True)
def my_route():
    ...
    return MyModel(...)
```

### Example app

For more complete examples see [example application](https://github.com/bauerji/flask_pydantic/tree/master/example_app).

### Configuration

The behaviour can be configured using flask's application config
`FLASK_PYDANTIC_VALIDATION_ERROR_STATUS_CODE` - response status code after validation error (defaults to `400`)

## Contributing

Feature requests and pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

- clone repository
  ```bash
  git clone https://github.com/bauerji/flask_pydantic.git
  cd flask_pydantic
  ```
- create virtual environment and activate it
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```
- install development requirements
  ```bash
  python3 -m pip install -r requirements/test.pip
  ```
- checkout new branch and make your desired changes (don't forget to update tests)
  ```bash
  git checkout -b <your_branch_name>
  ```
- run tests
  ```bash
  python3 -m pytest
  ```
- if tests fails on Black tests, make sure You have your code compliant with style of [Black formatter](https://github.com/psf/black)
- push your changes and create a pull request to master branch

## TODOs:

- header request parameters
- cookie request parameters
