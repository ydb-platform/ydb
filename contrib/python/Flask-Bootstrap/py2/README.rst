===============
Flask-Bootstrap
===============

.. image:: https://travis-ci.org/mbr/flask-bootstrap.png?branch=master
   :target: https://travis-ci.org/mbr/flask-bootstrap

Flask-Bootstrap packages `Bootstrap
<http://getbootstrap.com>`_ into an extension that mostly consists
of a blueprint named 'bootstrap'. It can also create links to serve Bootstrap
from a CDN and works with no boilerplate code in your application.

Usage
-----

Here is an example::

  from flask_bootstrap import Bootstrap

  [...]

  Bootstrap(app)

This makes some new templates available, containing blank pages that include all
bootstrap resources, and have predefined blocks where you can put your content.

As of version 3, Flask-Bootstrap has a `proper documentation
<http://pythonhosted.org /Flask-Bootstrap>`_, which you can check for more
details.
