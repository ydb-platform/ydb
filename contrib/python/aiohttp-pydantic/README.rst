Aiohttp pydantic - Aiohttp View to validate and parse request
=============================================================

.. image:: https://github.com/Maillol/aiohttp-pydantic/actions/workflows/install-package-and-test.yml/badge.svg
  :target: https://github.com/Maillol/aiohttp-pydantic/actions/workflows/install-package-and-test.yml
  :alt: CI Status

.. image:: https://img.shields.io/pypi/v/aiohttp-pydantic
  :target: https://img.shields.io/pypi/v/aiohttp-pydantic
  :alt: Latest PyPI package version

.. image:: https://codecov.io/gh/Maillol/aiohttp-pydantic/branch/main/graph/badge.svg
  :target: https://codecov.io/gh/Maillol/aiohttp-pydantic
  :alt: codecov.io status for master branch

Aiohttp pydantic is an `aiohttp view`_ to easily parse and validate request.
You define using the function annotations what your methods for handling HTTP verbs expects and Aiohttp pydantic parses the HTTP request
for you, validates the data, and injects that you want as parameters.


Features:

- Query string, request body, URL path and HTTP headers validation.
- Open API Specification generation.


How to install
--------------

.. code-block:: bash

    $ pip install aiohttp_pydantic

Example:
--------

.. code-block:: python3

    from typing import Optional

    from aiohttp import web
    from aiohttp_pydantic import PydanticView
    from pydantic import BaseModel

    # Use pydantic BaseModel to validate request body
    class ArticleModel(BaseModel):
        name: str
        nb_page: Optional[int]


    # Create your PydanticView and add annotations.
    class ArticleView(PydanticView):

        async def post(self, article: ArticleModel):
            return web.json_response({'name': article.name,
                                      'number_of_page': article.nb_page})

        async def get(self, with_comments: bool=False):
            return web.json_response({'with_comments': with_comments})


    app = web.Application()
    app.router.add_view('/article', ArticleView)
    web.run_app(app)


.. code-block:: bash

    $ curl -X GET http://127.0.0.1:8080/article?with_comments=a
    [
      {
        "in": "query string",
        "loc": [
          "with_comments"
        ],
        "msg": "Input should be a valid boolean, unable to interpret input",
        "input": "a",
        "type": "bool_parsing"
      }
    ]

    $ curl -X GET http://127.0.0.1:8080/article?with_comments=yes
    {"with_comments": true}

    $ curl -H "Content-Type: application/json" -X POST http://127.0.0.1:8080/article --data '{}'
    [
      {
        "in": "body",
        "loc": [
          "name"
        ],
        "msg": "Field required",
        "input": {},
        "type": "missing"
      },
      {
        "in": "body",
        "loc": [
          "nb_page"
        ],
        "msg": "Field required",
        "input": {},
        "type": "missing"
      }
    ]

    $ curl -H "Content-Type: application/json" -X POST http://127.0.0.1:8080/article --data '{"name": "toto", "nb_page": "3"}'
    {"name": "toto", "number_of_page": 3}


Example using view function handler
-----------------------------------

.. code-block:: python3


    from typing import Optional

    from aiohttp import web
    from aiohttp_pydantic.decorator import inject_params
    from pydantic import BaseModel


    # Use pydantic BaseModel to validate request body
    class ArticleModel(BaseModel):
        name: str
        nb_page: Optional[int]


    # Create your function decorated by 'inject_params' and add annotations.
    @inject_params
    async def post(article: ArticleModel):
        return web.json_response({'name': article.name,
                                  'number_of_page': article.nb_page})


    # If you need request
    @inject_params.and_request
    async def get(request, with_comments: bool = False):
        request.app["logger"]("OK")
        return web.json_response({'with_comments': with_comments})


    app = web.Application()
    app["logger"] = print
    app.router.add_post('/article', post)
    app.router.add_get('/article', get)
    web.run_app(app)


API:
----

Inject Path Parameters
~~~~~~~~~~~~~~~~~~~~~~

To declare a path parameter, you must declare your argument as a `positional-only parameters`_:


Example:

.. code-block:: python3

    class AccountView(PydanticView):
        async def get(self, customer_id: str, account_id: str, /):
            ...

    app = web.Application()
    app.router.add_get('/customers/{customer_id}/accounts/{account_id}', AccountView)

Inject Query String Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To declare a query parameter, you must declare your argument as a simple argument:


.. code-block:: python3

    class AccountView(PydanticView):
        async def get(self, customer_id: Optional[str] = None):
            ...

    app = web.Application()
    app.router.add_get('/customers', AccountView)


A query string parameter is generally optional and we do not want to force the user to set it in the URL.
It's recommended to define a default value. It's possible to get a multiple value for the same parameter using
the List type

.. code-block:: python3

    from typing import List
    from pydantic import Field

    class AccountView(PydanticView):
        async def get(self, tags: List[str] = Field(default_factory=list)):
            ...

    app = web.Application()
    app.router.add_get('/customers', AccountView)


Inject Request Body
~~~~~~~~~~~~~~~~~~~

To declare a body parameter, you must declare your argument as a simple argument annotated with `pydantic Model`_.


.. code-block:: python3

    class Customer(BaseModel):
        first_name: str
        last_name: str

    class CustomerView(PydanticView):
        async def post(self, customer: Customer):
            ...

    app = web.Application()
    app.router.add_view('/customers', CustomerView)

Inject HTTP headers
~~~~~~~~~~~~~~~~~~~

To declare a HTTP headers parameter, you must declare your argument as a `keyword-only argument`_.


.. code-block:: python3

    class CustomerView(PydanticView):
        async def get(self, *, authorization: str, expire_at: datetime):
            ...

    app = web.Application()
    app.router.add_view('/customers', CustomerView)


.. _positional-only parameters: https://www.python.org/dev/peps/pep-0570/
.. _pydantic Model: https://pydantic-docs.helpmanual.io/usage/models/
.. _keyword-only argument: https://www.python.org/dev/peps/pep-3102/


File Upload
-----------

You can receive files in addition to Pydantic data in your views. Here’s an example of how to use it:
Usage Example

Suppose you want to create an API that accepts a book (with a title and a number of pages) as well as two files
representing the pages of the book. Here’s how you can do it:

.. code-block:: python3

    from aiohttp import web
    from aiohttp_pydantic import PydanticView
    from aiohttp_pydantic.uploaded_file import UploadedFile
    from pydantic import BaseModel

    class BookModel(BaseModel):
        title: str
        nb_page: int

    class BookAndUploadFileView(PydanticView):
        async def post(self, book: BookModel, page_1: UploadedFile, page_2: UploadedFile):
            content_1 = (await page_1.read()).decode("utf-8")
            content_2 = (await page_2.read()).decode("utf-8")
            return web.json_response(
                {"book": book.model_dump(), "content_1": content_1, "content_2": content_2},
                status=201,
            )

Implementation Details
~~~~~~~~~~~~~~~~~~~~~~

Files are represented by instances of UploadedFile, which wrap an `aiohttp.BodyPartReader`_.
UploadedFile exposes the read() and read_chunk() methods, allowing you to read the content of uploaded files asynchronously. You can use read() to get the complete content or read_chunk() to read chunks of data at a time.

Constraints to Consider
~~~~~~~~~~~~~~~~~~~~~~~

1 - Argument Order:  If you use both Pydantic models and UploadedFile, you must always define BaseModel
type arguments before UploadedFile type arguments. This ensures proper processing of the data.

2 - File Reading Order: UploadedFile instances must be read in the order they are declared in the method.
Since files are not pre-loaded in memory or on disk, it is important to respect this order.
If the reading order is not respected, a MultipartReadingError is raised.


.. _aiohttp.BodyPartReader: https://docs.aiohttp.org/en/stable/multipart_reference.html#aiohttp.BodyPartReader



Add route to generate Open Api Specification (OAS)
--------------------------------------------------

aiohttp_pydantic provides a sub-application to serve a route to generate Open Api Specification
reading annotation in your PydanticView. Use *aiohttp_pydantic.oas.setup()* to add the sub-application

.. code-block:: python3

    from aiohttp import web
    from aiohttp_pydantic import oas


    app = web.Application()
    oas.setup(app)

By default, the route to display the Open Api Specification is /oas but you can change it using
*url_prefix* parameter


.. code-block:: python3

    oas.setup(app, url_prefix='/spec-api')

If you want generate the Open Api Specification from specific aiohttp sub-applications.
on the same route, you must use *apps_to_expose* parameter.


.. code-block:: python3

    from aiohttp import web
    from aiohttp_pydantic import oas

    app = web.Application()
    sub_app_1 = web.Application()
    sub_app_2 = web.Application()

    oas.setup(app, apps_to_expose=[sub_app_1, sub_app_2])


You can change the title or the version of the generated open api specification using
*title_spec* and *version_spec* parameters:


.. code-block:: python3

    oas.setup(app, title_spec="My application", version_spec="1.2.3")


Add annotation to define response content
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The module aiohttp_pydantic.oas.typing provides class to annotate a
response content.

For example *r200[List[Pet]]* means the server responses with
the status code 200 and the response content is a List of Pet where Pet will be
defined using a pydantic.BaseModel

The docstring of methods will be parsed to fill the descriptions in the
Open Api Specification.


.. code-block:: python3

    from aiohttp_pydantic import PydanticView
    from aiohttp_pydantic.oas.typing import r200, r201, r204, r404


    class Pet(BaseModel):
        id: int
        name: str


    class Error(BaseModel):
        error: str


    class PetCollectionView(PydanticView):
        async def get(self) -> r200[List[Pet]]:
            """
            Find all pets

            Tags: pet
            """
            pets = self.request.app["model"].list_pets()
            return web.json_response([pet.dict() for pet in pets])

        async def post(self, pet: Pet) -> r201[Pet]:
            """
            Add a new pet to the store

            Tags: pet
            Status Codes:
                201: The pet is created
            """
            self.request.app["model"].add_pet(pet)
            return web.json_response(pet.dict())


    class PetItemView(PydanticView):
        async def get(self, id: int, /) -> Union[r200[Pet], r404[Error]]:
            """
            Find a pet by ID

            Tags: pet
            Status Codes:
                200: Successful operation
                404: Pet not found
            """
            pet = self.request.app["model"].find_pet(id)
            return web.json_response(pet.dict())

        async def put(self, id: int, /, pet: Pet) -> r200[Pet]:
            """
            Update an existing pet

            Tags: pet
            Status Codes:
                200: successful operation
            """
            self.request.app["model"].update_pet(id, pet)
            return web.json_response(pet.dict())

        async def delete(self, id: int, /) -> r204:
            self.request.app["model"].remove_pet(id)
            return web.Response(status=204)


Patching JSON Schema
~~~~~~~~~~~~~~~~~~~~

You can define a function that modifies a Pydantic-generated schema in-place:

.. code-block:: python3

    def pydantic_schema_to_oas_3_0(schema: dict) -> None:
        """
        Modify the JSON schema in place.

        This function receives a Pydantic-generated JSON schema as a dictionary,
        and can adjust or enhance it before it is used in the OpenAPI specification.
        """

Once defined, you can register it:

.. code-block:: python3

    oas.pydantic_schema_to_oas.add_or_replace_translater(
        "3.0.0", pydantic_schema_to_oas_3_0
    )


Group parameters
----------------

If your method has lot of parameters you can group them together inside one or several Groups.


.. code-block:: python3

    from aiohttp_pydantic.injectors import Group

    class Pagination(Group):
        page_num: int = 1
        page_size: int = 15


    class ArticleView(PydanticView):

        async def get(self, page: Pagination):
            articles = Article.get(page.page_num, page.page_size)
            ...


The parameters page_num and page_size are expected in the query string, and
set inside a Pagination object passed as page parameter.

The code above is equivalent to:


.. code-block:: python3

    class ArticleView(PydanticView):

        async def get(self, page_num: int = 1, page_size: int = 15):
            articles = Article.get(page_num, page_size)
            ...


You can add methods or properties to your Group.


.. code-block:: python3

    class Pagination(Group):
        page_num: int = 1
        page_size: int = 15

        @property
        def num(self):
            return self.page_num

        @property
        def size(self):
            return self.page_size

        def slice(self):
            return slice(self.num, self.size)


    class ArticleView(PydanticView):

        async def get(self, page: Pagination):
            articles = Article.get(page.num, page.size)
            ...


Custom Validation error
-----------------------

You can redefine the on_validation_error hook in your PydanticView

.. code-block:: python3

    class PetView(PydanticView):

        async def on_validation_error(self,
                                      exception: ValidationError,
                                      context: str):
            errors = exception.errors()
            for error in errors:
                error["in"] = context  # context is "body", "headers", "path" or "query string"
                error["custom"] = "your custom field ..."
            return json_response(data=errors, status=400)



If you use function based view:

.. code-block:: python3


    async def custom_error(exception: ValidationError,
                           context: str):
        errors = exception.errors()
        for error in errors:
            error["in"] = context  # context is "body", "headers", "path" or "query string"
            error["custom"] = "your custom field ..."
        return json_response(data=errors, status=400)


    @inject_params(on_validation_error=custom_error)
    async def get(with_comments: bool = False):
        ...

    @inject_params.and_request(on_validation_error=custom_error)
    async def get(request, with_comments: bool = False):
        ...


A tip to use the same error handling on each view


.. code-block:: python3

    inject_params = inject_params(on_validation_error=custom_error)


    @inject_params
    async def post(article: ArticleModel):
        return web.json_response({'name': article.name,
                                  'number_of_page': article.nb_page})


    @inject_params.and_request
    async def get(request, with_comments: bool = False):
        return web.json_response({'with_comments': with_comments})




Add security to the endpoints
-----------------------------

aiohttp_pydantic provides a basic way to add security to the endpoints. You can define the security
on the setup level using the *security* parameter and then mark view methods that will require this security schema.

.. code-block:: python3

    from aiohttp import web
    from aiohttp_pydantic import oas


    app = web.Application()
    oas.setup(app, security={"APIKeyHeader": {"type": "apiKey", "in": "header", "name": "Authorization"}})


And then mark the view method with the *security* descriptor


.. code-block:: python3


    from aiohttp_pydantic import PydanticView
    from aiohttp_pydantic.oas.typing import r200, r201, r204, r404


    class Pet(BaseModel):
        id: int
        name: str


    class Error(BaseModel):
        error: str


    class PetCollectionView(PydanticView):
        async def get(self) -> r200[List[Pet]]:
            """
            Find all pets

            Security: APIKeyHeader
            Tags: pet
            """
            pets = self.request.app["model"].list_pets()
            return web.json_response([pet.dict() for pet in pets])

        async def post(self, pet: Pet) -> r201[Pet]:
            """
            Add a new pet to the store

            Tags: pet
            Status Codes:
                201: The pet is created
            """
            self.request.app["model"].add_pet(pet)
            return web.json_response(pet.dict())


Demo
----

Have a look at `demo`_ for a complete example

.. code-block:: bash

    git clone https://github.com/Maillol/aiohttp-pydantic.git
    cd aiohttp-pydantic
    pip install .
    python -m demo

Go to http://127.0.0.1:8080/oas

You can generate the OAS in a json or yaml file using the aiohttp_pydantic.oas command:

.. code-block:: bash

    python -m aiohttp_pydantic.oas demo.main

.. code-block:: bash

    $ python3 -m aiohttp_pydantic.oas  --help
    usage: __main__.py [-h] [-b FILE] [-o FILE] [-f FORMAT] [APP [APP ...]]

    Generate Open API Specification

    positional arguments:
      APP                   The name of the module containing the asyncio.web.Application. By default the variable named
                            'app' is loaded but you can define an other variable name ending the name of module with :
                            characters and the name of variable. Example: my_package.my_module:my_app If your
                            asyncio.web.Application is returned by a function, you can use the syntax:
                            my_package.my_module:my_app()

    optional arguments:
      -h, --help            show this help message and exit
      -b FILE, --base-oas-file FILE
                            A file that will be used as base to generate OAS
      -o FILE, --output FILE
                            File to write the output
      -f FORMAT, --format FORMAT
                            The output format, can be 'json' or 'yaml' (default is json)


.. _demo: https://github.com/Maillol/aiohttp-pydantic/tree/main/demo
.. _aiohttp view: https://docs.aiohttp.org/en/stable/web_quickstart.html#class-based-views
