Flask-SSLify
============

This is a simple Flask extension that configures your Flask application to redirect
all incoming requests to HTTPS.

Redirects only occur when ``app.debug`` is ``False``.

Usage
-----

Usage is pretty simple::

    from flask import Flask
    from flask_sslify import SSLify

    app = Flask(__name__)
    sslify = SSLify(app)


If you make an HTTP request, it will automatically redirect::

    $ curl -I http://secure-samurai.herokuapp.com/
    HTTP/1.1 302 FOUND
    Content-length: 281
    Content-Type: text/html; charset=utf-8
    Date: Sun, 29 Apr 2012 21:39:36 GMT
    Location: https://secure-samurai.herokuapp.com/
    Server: gunicorn/0.14.2
    Strict-Transport-Security: max-age=31536000
    Connection: keep-alive


HTTP Strict Transport Security
------------------------------

Flask-SSLify also provides your application with an HSTS policy.

By default, HSTS is set for *one year* (31536000 seconds).

You can change the duration by passing the ``age`` parameter::

    sslify = SSLify(app, age=300)

If you'd like to include subdomains in your HSTS policy, set the ``subdomains`` parameter::

    sslify = SSLify(app, subdomains=True)


Or by including ``SSLIFY_SUBDOMAINS`` in your app's config.


HTTP 301 Redirects
------------------

By default, the redirect is issued with a HTTP 302 response. You can change that to a HTTP 301 response
by passing the ``permanent`` parameter::

    sslify = SSLify(app, permanent=True)

Or by including ``SSLIFY_PERMANENT`` in your app's config.


Exclude Certain Paths from Being Redirected
-------------------------------------------
You can exlude a path that starts with given string by including a list called ``skips``::
 
     sslify = SSLify(app, skips=['mypath', 'anotherpath'])

Or by including ``SSLIFY_SKIPS`` in your app's config.


Install
-------

Installation is simple too::

    $ pip install Flask-SSLify
    
    
Security consideration using basic auth
---------------------------------------

When using basic auth, it is important that the redirect occurs before the user is prompted for
credentials. Flask-SSLify registers a ``before_request`` handler, to make sure this handler gets
executed before credentials are entered it is advisable to not prompt for any authentication
inside a ``before_request`` handler.

The example found at http://flask.pocoo.org/snippets/8/ works nicely, as the view function's
decorator will never have an effect before the ``before_request`` hooks are executed.
