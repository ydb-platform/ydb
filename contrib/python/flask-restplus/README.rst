==============
Flask RestPlus
==============

.. image:: https://secure.travis-ci.org/noirbizarre/flask-restplus.svg?tag=0.13.0
    :target: https://travis-ci.org/noirbizarre/flask-restplus?tag=0.13.0
    :alt: Build status
.. image:: https://coveralls.io/repos/noirbizarre/flask-restplus/badge.svg?tag=0.13.0
    :target: https://coveralls.io/r/noirbizarre/flask-restplus?tag=0.13.0
    :alt: Code coverage
.. image:: https://readthedocs.org/projects/flask-restplus/badge/?version=0.13.0
    :target: https://flask-restplus.readthedocs.io/en/0.13.0/
    :alt: Documentation status
.. image:: https://img.shields.io/pypi/l/flask-restplus.svg
    :target: https://pypi.org/project/flask-restplus
    :alt: License
.. image:: https://img.shields.io/pypi/pyversions/flask-restplus.svg
    :target: https://pypi.org/project/flask-restplus
    :alt: Supported Python versions
.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/noirbizarre/flask-restplus
   :target: https://gitter.im/noirbizarre/flask-restplus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Flask-RESTPlus is an extension for `Flask`_ that adds support for quickly building REST APIs.
Flask-RESTPlus encourages best practices with minimal setup.
If you are familiar with Flask, Flask-RESTPlus should be easy to pick up.
It provides a coherent collection of decorators and tools to describe your API
and expose its documentation properly using `Swagger`_.


Compatibility
=============

Flask-RestPlus requires Python 2.7 or 3.4+.


Installation
============

You can install Flask-Restplus with pip:

.. code-block:: console

    $ pip install flask-restplus

or with easy_install:

.. code-block:: console

    $ easy_install flask-restplus


Quick start
===========

With Flask-Restplus, you only import the api instance to route and document your endpoints.

.. code-block:: python

    from flask import Flask
    from flask_restplus import Api, Resource, fields

    app = Flask(__name__)
    api = Api(app, version='1.0', title='TodoMVC API',
        description='A simple TodoMVC API',
    )

    ns = api.namespace('todos', description='TODO operations')

    todo = api.model('Todo', {
        'id': fields.Integer(readOnly=True, description='The task unique identifier'),
        'task': fields.String(required=True, description='The task details')
    })


    class TodoDAO(object):
        def __init__(self):
            self.counter = 0
            self.todos = []

        def get(self, id):
            for todo in self.todos:
                if todo['id'] == id:
                    return todo
            api.abort(404, "Todo {} doesn't exist".format(id))

        def create(self, data):
            todo = data
            todo['id'] = self.counter = self.counter + 1
            self.todos.append(todo)
            return todo

        def update(self, id, data):
            todo = self.get(id)
            todo.update(data)
            return todo

        def delete(self, id):
            todo = self.get(id)
            self.todos.remove(todo)


    DAO = TodoDAO()
    DAO.create({'task': 'Build an API'})
    DAO.create({'task': '?????'})
    DAO.create({'task': 'profit!'})


    @ns.route('/')
    class TodoList(Resource):
        '''Shows a list of all todos, and lets you POST to add new tasks'''
        @ns.doc('list_todos')
        @ns.marshal_list_with(todo)
        def get(self):
            '''List all tasks'''
            return DAO.todos

        @ns.doc('create_todo')
        @ns.expect(todo)
        @ns.marshal_with(todo, code=201)
        def post(self):
            '''Create a new task'''
            return DAO.create(api.payload), 201


    @ns.route('/<int:id>')
    @ns.response(404, 'Todo not found')
    @ns.param('id', 'The task identifier')
    class Todo(Resource):
        '''Show a single todo item and lets you delete them'''
        @ns.doc('get_todo')
        @ns.marshal_with(todo)
        def get(self, id):
            '''Fetch a given resource'''
            return DAO.get(id)

        @ns.doc('delete_todo')
        @ns.response(204, 'Todo deleted')
        def delete(self, id):
            '''Delete a task given its identifier'''
            DAO.delete(id)
            return '', 204

        @ns.expect(todo)
        @ns.marshal_with(todo)
        def put(self, id):
            '''Update a task given its identifier'''
            return DAO.update(id, api.payload)


    if __name__ == '__main__':
        app.run(debug=True)


Contributors
============

Flask-RESTPlus is brought to you by @noirbizarre. Since early 2019 @SteadBytes,
@a-luna, @j5awry, @ziirish volunteered to help @noirbizarre keep the project up
and running.
Of course everyone is welcome to contribute and we will be happy to review your
PR's or answer to your issues.


Documentation
=============

The documentation is hosted `on Read the Docs <http://flask-restplus.readthedocs.io/en/latest/>`_


.. _Flask: http://flask.pocoo.org/
.. _Swagger: http://swagger.io/


Contribution
============
Want to contribute! That's awesome! Check out `CONTRIBUTING.rst! <https://github.com/noirbizarre/flask-restplus/blob/master/CONTRIBUTING.rst>`_