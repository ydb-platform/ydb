.. image:: https://img.shields.io/pypi/v/cherrypy.svg
   :target: https://pypi.org/project/cherrypy

.. image:: https://readthedocs.org/projects/cherrypy/badge/?version=latest
  :target: https://docs.cherrypy.org/en/latest/?badge=latest

.. image:: https://img.shields.io/badge/StackOverflow-CherryPy-blue.svg
   :target: https://stackoverflow.com/questions/tagged/cheroot+or+cherrypy

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

Welcome to the GitHub repository of `CherryPy <https://cherrypy.org/>`_!

CherryPy is a pythonic, object-oriented HTTP framework.

1. It allows building web applications in much the same way one would
   build any other object-oriented program.
2. This design results in less and more readable code being developed faster.
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
for the Python object model to be dynamically presented as a web site
and/or API.

While CherryPy is one of the easiest and most intuitive frameworks out
there, the prerequisite for understanding the `CherryPy
documentation <https://docs.cherrypy.org/en/latest/>`_ is that you have
a general understanding of Python and web development.
Additionally:

-  Tutorials are included in the repository:
   https://github.com/cherrypy/cherrypy/tree/master/cherrypy/tutorial
-  A general wiki at:
   https://github.com/cherrypy/cherrypy/wiki

If the docs are insufficient to address your needs, the CherryPy
community has several `avenues for support
<https://docs.cherrypy.org/en/latest/support.html>`_.

Contributing
------------

Please follow the `contribution guidelines
<https://docs.cherrypy.org/en/latest/contribute.html>`_.
And by all means, absorb the `Zen of
CherryPy <https://github.com/cherrypy/cherrypy/wiki/The-Zen-of-CherryPy>`_.
