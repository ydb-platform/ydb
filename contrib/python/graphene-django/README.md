# ![Graphene Logo](http://graphene-python.org/favicon.png) Graphene-Django

[![build][build-image]][build-url]
[![pypi][pypi-image]][pypi-url]
[![Anaconda-Server Badge][conda-image]][conda-url]
[![coveralls][coveralls-image]][coveralls-url]

[build-image]: https://github.com/graphql-python/graphene-django/workflows/Tests/badge.svg
[build-url]: https://github.com/graphql-python/graphene-django/actions
[pypi-image]: https://img.shields.io/pypi/v/graphene-django.svg?style=flat
[pypi-url]: https://pypi.org/project/graphene-django/
[coveralls-image]: https://coveralls.io/repos/github/graphql-python/graphene-django/badge.svg?branch=master
[coveralls-url]: https://coveralls.io/github/graphql-python/graphene-django?branch=master
[conda-image]: https://img.shields.io/conda/vn/conda-forge/graphene-django.svg
[conda-url]: https://anaconda.org/conda-forge/graphene-django

Graphene-Django is an open-source library that provides seamless integration between Django, a high-level Python web framework, and Graphene, a library for building GraphQL APIs. The library allows developers to create GraphQL APIs in Django quickly and efficiently while maintaining a high level of performance.

## Features

* Seamless integration with Django models
* Automatic generation of GraphQL schema
* Integration with Django's authentication and permission system
* Easy querying and filtering of data
* Support for Django's pagination system
* Compatible with Django's form and validation system
* Extensive documentation and community support

## Installation

To install Graphene-Django, run the following command:

```sh
pip install graphene-django
```

## Configuration

After installation, add 'graphene_django' to your Django project's `INSTALLED_APPS` list and define the GraphQL schema in your project's settings:

```python
INSTALLED_APPS = [
    # ...
    'graphene_django',
]

GRAPHENE = {
    'SCHEMA': 'myapp.schema.schema'
}
```

## Usage

To use Graphene-Django, create a `schema.py` file in your Django app directory and define your GraphQL types and queries:

```python
import graphene
from graphene_django import DjangoObjectType
from .models import MyModel

class MyModelType(DjangoObjectType):
    class Meta:
        model = MyModel

class Query(graphene.ObjectType):
    mymodels = graphene.List(MyModelType)

    def resolve_mymodels(self, info, **kwargs):
        return MyModel.objects.all()

schema = graphene.Schema(query=Query)
```

Then, expose the GraphQL API in your Django project's `urls.py` file:

```python
from django.urls import path
from graphene_django.views import GraphQLView
from . import schema

urlpatterns = [
    # ...
    path('graphql/', GraphQLView.as_view(graphiql=True)), # Given that schema path is defined in GRAPHENE['SCHEMA'] in your settings.py
]
```

## Testing

Graphene-Django provides support for testing GraphQL APIs using Django's test client. To create tests, create a `tests.py` file in your Django app directory and write your test cases:

```python
from django.test import TestCase
from graphene_django.utils.testing import GraphQLTestCase
from . import schema

class MyModelAPITestCase(GraphQLTestCase):
    GRAPHENE_SCHEMA = schema.schema

    def test_query_all_mymodels(self):
        response = self.query(
            '''
            query {
                mymodels {
                    id
                    name
                }
            }
            '''
        )

        self.assertResponseNoErrors(response)
        self.assertEqual(len(response.data['mymodels']), MyModel.objects.count())
```

## Contributing

Contributions to Graphene-Django are always welcome! To get started, check the repository's [issue tracker](https://github.com/graphql-python/graphene-django/issues) and [contribution guidelines](https://github.com/graphql-python/graphene-django/blob/main/CONTRIBUTING.md).

## License

Graphene-Django is released under the [MIT License](https://github.com/graphql-python/graphene-django/blob/main/LICENSE).

## Resources

* [Official GitHub Repository](https://github.com/graphql-python/graphene-django)
* [Graphene Documentation](http://docs.graphene-python.org/en/latest/)
* [Django Documentation](https://docs.djangoproject.com/en/stable/)
* [GraphQL Specification](https://spec.graphql.org/)
* [GraphiQL](https://github.com/graphql/graphiql) - An in-browser IDE for exploring GraphQL APIs
* [Graphene-Django Community](https://spectrum.chat/graphene) - Join the community to discuss questions and share ideas related to Graphene-Django

## Tutorials and Examples

* [Official Graphene-Django Tutorial](https://docs.graphene-python.org/projects/django/en/latest/tutorial-plain/)
* [Building a GraphQL API with Django and Graphene-Django](https://www.howtographql.com/graphql-python/0-introduction/)
* [Real-world example: Django, Graphene, and Relay](https://github.com/graphql-python/swapi-graphene)

## Related Projects

* [Graphene](https://github.com/graphql-python/graphene) - A library for building GraphQL APIs in Python
* [Graphene-SQLAlchemy](https://github.com/graphql-python/graphene-sqlalchemy) - Integration between Graphene and SQLAlchemy, an Object Relational Mapper (ORM) for Python
* [Graphene-File-Upload](https://github.com/lmcgartland/graphene-file-upload) - A package providing an Upload scalar for handling file uploads in Graphene
* [Graphene-Subscriptions](https://github.com/graphql-python/graphene-subscriptions) - A package for adding real-time subscriptions to Graphene-based GraphQL APIs

## Support

If you encounter any issues or have questions regarding Graphene-Django, feel free to [submit an issue](https://github.com/graphql-python/graphene-django/issues/new) on the official GitHub repository. You can also ask for help and share your experiences with the Graphene-Django community on [💬 Discord](https://discord.gg/Fftt273T79)

## Release Notes

* See [Releases page on github](https://github.com/graphql-python/graphene-django/releases)
