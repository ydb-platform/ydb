*******
webargs
*******

.. image:: https://badgen.net/pypi/v/webargs
    :target: https://pypi.org/project/webargs/
    :alt: PyPI version

.. image:: https://dev.azure.com/sloria/sloria/_apis/build/status/marshmallow-code.webargs?branchName=dev
    :target: https://dev.azure.com/sloria/sloria/_build/latest?definitionId=6&branchName=dev
    :alt: Build status

.. image:: https://readthedocs.org/projects/webargs/badge/
   :target: https://webargs.readthedocs.io/
   :alt: Documentation

.. image:: https://badgen.net/badge/marshmallow/2,3?list=1
    :target: https://marshmallow.readthedocs.io/en/latest/upgrading.html
    :alt: marshmallow 2/3 compatible

.. image:: https://badgen.net/badge/code%20style/black/000
    :target: https://github.com/ambv/black
    :alt: code style: black

Homepage: https://webargs.readthedocs.io/

webargs is a Python library for parsing and validating HTTP request objects, with built-in support for popular web frameworks, including Flask, Django, Bottle, Tornado, Pyramid, webapp2, Falcon, and aiohttp.

.. code-block:: python

    from flask import Flask
    from webargs import fields
    from webargs.flaskparser import use_args

    app = Flask(__name__)


    @app.route("/")
    @use_args({"name": fields.Str(required=True)})
    def index(args):
        return "Hello " + args["name"]


    if __name__ == "__main__":
        app.run()

    # curl http://localhost:5000/\?name\='World'
    # Hello World

Install
=======

::

    pip install -U webargs

webargs supports Python >= 2.7 or >= 3.5.


Documentation
=============

Full documentation is available at https://webargs.readthedocs.io/.

Support webargs
===============

webargs is maintained by a group of 
`volunteers <https://webargs.readthedocs.io/en/latest/authors.html>`_.
If you'd like to support the future of the project, please consider
contributing to our Open Collective:

.. image:: https://opencollective.com/marshmallow/donate/button.png
    :target: https://opencollective.com/marshmallow
    :width: 200
    :alt: Donate to our collective

Professional Support
====================

Professionally-supported webargs is available through the
`Tidelift Subscription <https://tidelift.com/subscription/pkg/pypi-webargs?utm_source=pypi-webargs&utm_medium=referral&utm_campaign=readme>`_.

Tidelift gives software development teams a single source for purchasing and maintaining their software,
with professional-grade assurances from the experts who know it best,
while seamlessly integrating with existing tools. [`Get professional support`_]

.. _`Get professional support`: https://tidelift.com/subscription/pkg/pypi-webargs?utm_source=pypi-webargs&utm_medium=referral&utm_campaign=readme

.. image:: https://user-images.githubusercontent.com/2379650/45126032-50b69880-b13f-11e8-9c2c-abd16c433495.png
    :target: https://tidelift.com/subscription/pkg/pypi-webargs?utm_source=pypi-webargs&utm_medium=referral&utm_campaign=readme
    :alt: Get supported marshmallow with Tidelift

Security Contact Information
============================

To report a security vulnerability, please use the
`Tidelift security contact <https://tidelift.com/security>`_.
Tidelift will coordinate the fix and disclosure.

Project Links
=============

- Docs: https://webargs.readthedocs.io/
- Changelog: https://webargs.readthedocs.io/en/latest/changelog.html
- Contributing Guidelines: https://webargs.readthedocs.io/en/latest/contributing.html
- PyPI: https://pypi.python.org/pypi/webargs
- Issues: https://github.com/marshmallow-code/webargs/issues


License
=======

MIT licensed. See the `LICENSE <https://github.com/marshmallow-code/webargs/blob/dev/LICENSE>`_ file for more details.
