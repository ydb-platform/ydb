Flask-AppConfig
===============

Allows you to configure an application using pre-set methods.

.. code-block:: python

    from flask_appconfig import AppConfig

    def create_app(configfile=None):
        app = Flask('myapp')
        AppConfig(app, configfile)
        return app

The application returned by ``create_app`` will, in order:

1. Load default settings from a module called ``myapp.default_config``, if it
   exists. (method described in
   http://flask.pocoo.org/docs/config/#configuring-from-files )
2. Load settings from a configuration file whose name is given in the
   environment variable ``MYAPP_CONFIG`` (see link from 1.).
3. Load json or string values directly from environment variables that start
   with a prefix of ``MYAPP_``, i.e. setting ``MYAPP_SQLALCHEMY_ECHO=true``
   will cause the setting of ``SQLALCHEMY_ECHO`` to be ``True``.

Any of these behaviors can be altered or disabled by passing the appropriate
options to the constructor or ``init_app()``.


Heroku support
--------------

Flask-AppConfig supports configuring a number of services through
``HerokuConfig``:

.. code-block:: python

    from flask_appconfig import HerokuConfig

    def create_app(configfile=None):
        app = Flask('myapp')
        HerokuConfig(app, configfile)
        return app

Works like the example above, but environment variables set by various Heroku
addons will be parsed as json and converted to configuration variables
accordingly. Forexample, when enabling `Mailgun
<https://addons.heroku.com/mailgun>`_, the configuration of `Flask-Mail
<http://pythonhosted.org/Flask-Mail/>`_ will be automatically be set correctly.


Using "ENV-only"
----------------

If you only want to use the environment-parsing functions of Flask-AppConfig,
the appropriate functions are exposed:

.. code-block:: python

    from flask_appconfig.heroku import from_heroku_envvars
    from flask_appconfig.env import from_envvars

    # from environment variables. note that you need to set the prefix, as
    # no auto-detection can be done without an app object
    from_envvars(app.config, prefix=app.name.upper() + '_')

    # also possible: parse heroku configuration values
    # any dict-like object will do as the first parameter
    from_heroku_envvars(app.config)


Installation
------------

Via `PyPI <http://pypi.python.org/pypi/flask-appconfig>`_::

    $ pip install flask-appconfig

Requires Python 2.7.


flask utility
-------------

If you want to get started quickly without thinking a lot about writing a run
script, the ``flask`` utility supports the ``create_app``/factory pattern::

    $ flask --app=myapp dev

This will import a module ``myapp``, and call ``myapp.run(debug=True)``.

Other options can come in handy as well::

    $ flask --app=myapp dev -S -p 8000

Runs the app on port 8080, with SSL enabled. You can also set the ``FLASK_APP``
environment variable or set ``FLASK_APP`` inside ``.env`` and omit the
``--app`` parameter.

Note that the ``flask`` utility is subject to change, as it will conflict with
the CLI functionality of Flask 1.0. The API is currently kept close, but it
will see changes once Flask 1.0 is released.


Flask-Debug and Flask-DebugToolbar support
******************************************

``flask`` automatically activates Flask-Debug_ and Flask-DebugToolbar_ on
your application; this allows to have it installed locally while not having to
install any debug code in production. You can suppress this behavior with the
``-E``/``--no-flask-debug`` flag.

Note that these features are only enabled if you install either of these
extensions manually; they are not dependencies of Flask-Appconfig.

.. _Flask-Debug: https://github.com/mbr/flask-debug
.. _Flask-DebugToolbar: https://flask-debugtoolbar.readthedocs.org/


Thoughts on Configuration
-------------------------

There is a lot of ways to configure a Flask application and often times,
less-than-optimal ones are chosen in a hurry.

This extension aims to do three things:

1. Set a "standard" of doing configuration that is flexible and in-line with
   the official docs and (what I consider) good practices.
2. Make it as convenient as possible to provide these configuration methods in
   an application.
3. Auto-configure on Heroku as much as possible without sacrificing 1. and 2.

`12factor.net <http://12factor.net/>`_ seems to capture a good amount of good
thoughts on the issue and Flask-Appconfig should aid you in writing an
application that follows the principles laid out there.

Providing defaults
******************

Defaults should be included and overridable, without altering the file
containing the defaults.

Separate code and configuration
*******************************

It should be possible to install the app to a read-only (possibly system-wide)
location, without having to store configuration files (or, even worse,
configuration modules) inside its folders.

Environment variables and instance folders make this possible. As an added
benefit, configuration does not need to be stored alongside the code in version
control.

No code necessary for most deployments using the factory-method pattern
***********************************************************************

When deploying with gunicorn, passing ``myapp:create_app()`` suffices to create
an app instance, no boilerplate code to create the WSGI app should be necessary.

Multiple instances
******************

Running multiple apps inside the same interpreter should also be possible. While
this is slightly more complicated and may occasionally violate the "no-code"
guideline above, it's still straightforward by using configuration file
parameters.


Development
-----------
Flask-AppConfig is under "conceptional development". The API or semantics
may change in the future.

Send pull requests for more Heroku-apps to be supported. Send feedback via mail.

Changelog
---------

Backwards-incompatible changes, as they were introduced:

0.11
****
* The ``flaskdev`` tool has been replaced with ``flask``.
* Using the new ``flask`` tool auto-reloading will also change by default. If a
  syntax error is introduced to the code, the app will try to restart after two
  seconds by default, instead of crashing. This can be suppressed with the
  '--extended-reload 0' flag.
* If the app import fails, ``flask`` will add ``.`` to ``sys.path`` and try to
  to import once again.
* Experimental commands ``serve`` and ``db`` have been added.

0.4
***
* Environment variables are no longer prefixed with ``FLASK_`` by default, but
  rather use ``APPNAME_`` (with ``APPNAME`` being the applications name in
  uppercase).
* ``MYAPP_SETTINGS`` became ``MYAPP_CONFIG``, ``default_settings`` became
  ``default_config``.
