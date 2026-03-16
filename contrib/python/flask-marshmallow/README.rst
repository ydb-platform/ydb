*****************
Flask-Marshmallow
*****************

|pypi-package| |build-status| |docs| |marshmallow-support|

Flask + marshmallow for beautiful APIs
======================================

Flask-Marshmallow is a thin integration layer for `Flask`_ (a Python web framework) and `marshmallow`_ (an object serialization/deserialization library) that adds additional features to marshmallow, including URL and Hyperlinks fields for HATEOAS-ready APIs. It also (optionally) integrates with `Flask-SQLAlchemy <http://flask-sqlalchemy.pocoo.org/>`_.

Get it now
----------
::

    pip install flask-marshmallow


Create your app.

.. code-block:: python

    from flask import Flask
    from flask_marshmallow import Marshmallow

    app = Flask(__name__)
    ma = Marshmallow(app)

Write your models.

.. code-block:: python

    from your_orm import Model, Column, Integer, String, DateTime


    class User(Model):
        email = Column(String)
        password = Column(String)
        date_created = Column(DateTime, auto_now_add=True)


Define your output format with marshmallow.

.. code-block:: python


    class UserSchema(ma.Schema):
        email = ma.Email()
        date_created = ma.DateTime()

        # Smart hyperlinking
        _links = ma.Hyperlinks(
            {
                "self": ma.URLFor("user_detail", values=dict(id="<id>")),
                "collection": ma.URLFor("users"),
            }
        )


    user_schema = UserSchema()
    users_schema = UserSchema(many=True)


Output the data in your views.

.. code-block:: python

    @app.route("/api/users/")
    def users():
        all_users = User.all()
        return users_schema.dump(all_users)


    @app.route("/api/users/<id>")
    def user_detail(id):
        user = User.get(id)
        return user_schema.dump(user)


    # {
    #     "email": "fred@queen.com",
    #     "date_created": "Fri, 25 Apr 2014 06:02:56 -0000",
    #     "_links": {
    #         "self": "/api/users/42",
    #         "collection": "/api/users/"
    #     }
    # }


http://flask-marshmallow.readthedocs.io/
========================================

Learn More
==========

To learn more about marshmallow, check out its `docs <http://marshmallow.readthedocs.io/en/latest/>`_.



Project Links
=============

- Docs: https://flask-marshmallow.readthedocs.io/
- Changelog: http://flask-marshmallow.readthedocs.io/en/latest/changelog.html
- PyPI: https://pypi.org/project/flask-marshmallow/
- Issues: https://github.com/marshmallow-code/flask-marshmallow/issues

License
=======

MIT licensed. See the bundled `LICENSE <https://github.com/marshmallow-code/flask-marshmallow/blob/master/LICENSE>`_ file for more details.


.. _Flask: http://flask.pocoo.org
.. _marshmallow: http://marshmallow.readthedocs.io

.. |pypi-package| image:: https://badgen.net/pypi/v/flask-marshmallow
    :target: https://pypi.org/project/flask-marshmallow/
    :alt: Latest version

.. |build-status| image:: https://github.com/marshmallow-code/flask-marshmallow/actions/workflows/build-release.yml/badge.svg
    :target: https://github.com/marshmallow-code/flask-marshmallow/actions/workflows/build-release.yml
    :alt: Build status

.. |docs| image:: https://readthedocs.org/projects/flask-marshmallow/badge/
   :target: https://flask-marshmallow.readthedocs.io/
   :alt: Documentation

.. |marshmallow-support| image:: https://badgen.net/badge/marshmallow/3,4?list=1
    :target: https://marshmallow.readthedocs.io/en/latest/upgrading.html
    :alt: marshmallow 3|4 compatible
