Please read
`UPGRADE-v2.0.md <https://github.com/graphql-python/graphene/blob/master/UPGRADE-v2.0.md>`__
to learn how to upgrade to Graphene ``2.0``.

--------------

|Graphene Logo| Graphene-SQLAlchemy |Build Status| |PyPI version| |Coverage Status|
===================================================================================

A `SQLAlchemy <http://www.sqlalchemy.org/>`__ integration for
`Graphene <http://graphene-python.org/>`__.

Installation
------------

For instaling graphene, just run this command in your shell

.. code:: bash

    pip install "graphene-sqlalchemy>=2.0"

Examples
--------

Here is a simple SQLAlchemy model:

.. code:: python

    from sqlalchemy import Column, Integer, String
    from sqlalchemy.orm import backref, relationship

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class UserModel(Base):
        __tablename__ = 'department'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        last_name = Column(String)

To create a GraphQL schema for it you simply have to write the
following:

.. code:: python

    from graphene_sqlalchemy import SQLAlchemyObjectType

    class User(SQLAlchemyObjectType):
        class Meta:
            model = UserModel

    class Query(graphene.ObjectType):
        users = graphene.List(User)

        def resolve_users(self, info):
            query = User.get_query(info)  # SQLAlchemy query
            return query.all()

    schema = graphene.Schema(query=Query)

Then you can simply query the schema:

.. code:: python

    query = '''
        query {
          users {
            name,
            lastName
          }
        }
    '''
    result = schema.execute(query, context_value={'session': db_session})

To learn more check out the following `examples <examples/>`__:

-  **Full example**: `Flask SQLAlchemy
   example <examples/flask_sqlalchemy>`__

Contributing
------------

After cloning this repo, ensure dependencies are installed by running:

.. code:: sh

    python setup.py install

After developing, the full test suite can be evaluated by running:

.. code:: sh

    python setup.py test # Use --pytest-args="-v -s" for verbose mode

.. |Graphene Logo| image:: http://graphene-python.org/favicon.png
.. |Build Status| image:: https://travis-ci.org/graphql-python/graphene-sqlalchemy.svg?branch=master
   :target: https://travis-ci.org/graphql-python/graphene-sqlalchemy
.. |PyPI version| image:: https://badge.fury.io/py/graphene-sqlalchemy.svg
   :target: https://badge.fury.io/py/graphene-sqlalchemy
.. |Coverage Status| image:: https://coveralls.io/repos/graphql-python/graphene-sqlalchemy/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/graphql-python/graphene-sqlalchemy?branch=master
