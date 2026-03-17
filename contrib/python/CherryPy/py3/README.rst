.. image:: https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/banner-direct.svg
   :target: https://github.com/vshymanskyy/StandWithUkraine/blob/main/docs/README.md
   :alt: SWUbanner

.. image:: https://img.shields.io/pypi/v/cherrypy.svg
   :target: https://pypi.org/project/cherrypy

.. image:: https://tidelift.com/badges/package/pypi/CherryPy
   :target: https://tidelift.com/subscription/pkg/pypi-cherrypy?utm_source=pypi-cherrypy&utm_medium=readme
   :alt: CherryPy is available as part of the Tidelift Subscription

.. image:: https://img.shields.io/badge/Python%203%20only-pip%20install%20%22%3E%3D18.0.0%22-%234da45e.svg
   :target: https://python3statement.org/

.. image:: https://img.shields.io/badge/Python%203%20and%202-pip%20install%20%22%3C18.0.0%22-%2349a7e9.svg
   :target: https://python3statement.org/#sections40-timeline



.. image:: https://readthedocs.org/projects/cherrypy/badge/?version=latest
  :target: https://docs.cherrypy.dev/en/latest/?badge=latest

.. image:: https://img.shields.io/badge/StackOverflow-CherryPy-blue.svg
   :target: https://stackoverflow.com/questions/tagged/cheroot+or+cherrypy

.. image:: https://img.shields.io/badge/Mailing%20list-cherrypy--users-orange.svg
   :target: https://groups.google.com/group/cherrypy-users

.. image:: https://img.shields.io/gitter/room/cherrypy/cherrypy.svg
   :target: https://gitter.im/cherrypy/cherrypy

.. image:: https://img.shields.io/travis/cherrypy/cherrypy/master.svg?label=Linux%20build%20%40%20Travis%20CI
   :target: https://travis-ci.org/cherrypy/cherrypy

.. image:: https://circleci.com/gh/cherrypy/cherrypy/tree/master.svg?style=svg
   :target: https://circleci.com/gh/cherrypy/cherrypy/tree/master

.. image:: https://img.shields.io/appveyor/ci/CherryPy/cherrypy/master.svg?label=Windows%20build%20%40%20Appveyor
   :target: https://ci.appveyor.com/project/CherryPy/cherrypy/branch/master

.. image:: https://img.shields.io/badge/license-BSD-blue.svg?maxAge=3600
   :target: https://pypi.org/project/cheroot

.. image:: https://img.shields.io/pypi/pyversions/cherrypy.svg
   :target: https://pypi.org/project/cherrypy

.. image:: https://badges.github.io/stability-badges/dist/stable.svg
   :target: https://github.com/badges/stability-badges
   :alt: stable

.. image:: https://api.codacy.com/project/badge/Grade/48b11060b5d249dc86e52dac2be2c715
   :target: https://www.codacy.com/app/webknjaz/cherrypy-upstream?utm_source=github.com&utm_medium=referral&utm_content=cherrypy/cherrypy&utm_campaign=Badge_Grade

.. image:: https://codecov.io/gh/cherrypy/cherrypy/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/cherrypy/cherrypy
   :alt: codecov

Welcome to the GitHub repository of `CherryPy <https://cherrypy.dev>`_!

CherryPy is a pythonic, object-oriented HTTP framework.

1. It allows building web applications in much the same way one would
   build any other object-oriented program.
2. This design results in more concise and readable code developed faster.
   It's all just properties and methods.
3. It is now more than ten years old and has proven fast and very
   stable.
4. It is being used in production by many sites, from the simplest to
   the most demanding.
5. And perhaps most importantly, it is fun to work with :-)

Here's how easy it is to write "Hello World" in CherryPy:

.. code:: python

    import cherrypy

    class HelloWorld(object):
        @cherrypy.expose
        def index(self):
            return "Hello World!"

    cherrypy.quickstart(HelloWorld())

And it continues to work that intuitively when systems grow, allowing
for the Python object model to be dynamically presented as a website
and/or API.

While CherryPy is one of the easiest and most intuitive frameworks out
there, the prerequisite for understanding the `CherryPy
documentation <https://docs.cherrypy.dev>`_ is that you have
a general understanding of Python and web development.
Additionally:

-  Tutorials are included in the repository:
   https://github.com/cherrypy/cherrypy/tree/master/cherrypy/tutorial
-  A general wiki at:
   https://github.com/cherrypy/cherrypy/wiki

If the docs are insufficient to address your needs, the CherryPy
community has several `avenues for support
<https://docs.cherrypy.dev/en/latest/support.html>`_.

For Enterprise
--------------

CherryPy is available as part of the Tidelift Subscription.

The CherryPy maintainers and the maintainers of thousands of other packages
are working with Tidelift to deliver one enterprise subscription that covers
all of the open source you use.

`Learn more <https://tidelift.com/subscription/pkg/pypi-cherrypy?utm_source=pypi-cherrypy&utm_medium=referral&utm_campaign=github>`_.

Contributing
------------

Please follow the `contribution guidelines
<https://docs.cherrypy.dev/en/latest/contribute.html>`_.
And by all means, absorb the `Zen of
CherryPy <https://github.com/cherrypy/cherrypy/wiki/The-Zen-of-CherryPy>`_.
