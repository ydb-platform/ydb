uwsgiconf
=========
https://github.com/idlesign/uwsgiconf


|release| |lic| |coverage|

.. |release| image:: https://img.shields.io/pypi/v/uwsgiconf.svg
    :target: https://pypi.python.org/pypi/uwsgiconf

.. |lic| image:: https://img.shields.io/pypi/l/uwsgiconf.svg
    :target: https://pypi.python.org/pypi/uwsgiconf

.. |coverage| image:: https://img.shields.io/coveralls/idlesign/uwsgiconf/master.svg
    :target: https://coveralls.io/r/idlesign/uwsgiconf


Description
-----------

*Configure uWSGI from your Python code*

If you think you know uWSGI you're probably wrong. It is always more than you think it is.
There are so many subsystems and options_ (800+) it is difficult to even try to wrap your mind around.

.. _options: http://uwsgi-docs.readthedocs.io/en/latest/Options.html

**uwsgiconf** allowing to define uWSGI configurations in Python tries to improve things the following ways:

* It structures options for various subsystems using classes and methods;
* It uses docstrings and sane naming to facilitate navigation;
* It ships some useful presets to reduce boilerplate code;
* It encourages configuration reuse;
* It comes with CLI to facilitate configuration;
* It features easy to use and documented **uwsgi stub** Python module;
* It offers **runtime** package, similar to **uwsgidecorators**, but with more abstractions;
* It features integration with Django Framework;
* It is able to generate configuration files for Systemd, Upstart.
* It can use ``pyuwsgi``.


*Consider using IDE with autocompletion and docstings support to be more productive with uwsgiconf.*

By that time you already know that **uwsgiconf** is just another configuration method. Why_?

.. _Why: http://uwsgi-docs.readthedocs.io/en/latest/FAQ.html#why-do-you-support-multiple-methods-of-configuration


Overview
--------

Static configuration
~~~~~~~~~~~~~~~~~~~~

Let's make ``uwsgicfg.py``. There we configure uWSGI using nice ``PythonSection`` preset to run our web app.

.. code-block:: python

    from uwsgiconf.config import configure_uwsgi
    from uwsgiconf.presets.nice import PythonSection


    def get_configurations():
        """This should return one or more Section or Configuration objects.
        In such a way you can configure more than one uWSGI instance in the same place.

        Here we'll define just one configuration section, which
        will instruct uWSGI to serve WSGI application (from wsgi.py module)
        on http://127.0.0.1:8000. We use .bootstrap shortcut method
        to construct our configuration section object.

        """
        return PythonSection.bootstrap('http://127.0.0.1:8000', wsgi_module='/home/idle/myapp/wsgi.py')


    # Almost done. One more thing:
    configure_uwsgi(get_configurations)



1. Now if you want to generate ``myconf.ini`` file and use it for uWSGI manually you can do it with:

    .. code-block:: bash

        $ uwsgiconf compile > myconf.ini
        $ uwsgi myconf.ini

2. Or use ``uwsgiconf`` to automatically spawn uWSGI processes for configurations defined in your module:

    .. code-block:: bash

        $ uwsgiconf run


**Note:** ``uwsgiconf`` CLI requires ``click`` package available (can be installed with ``uwsgiconf``).


Runtime configuration
~~~~~~~~~~~~~~~~~~~~~

**uwsgiconf** comes with ``runtime`` package which is similar to **uwsgidecorators** but
offers different abstractions to provide useful shortcuts and defaults.

These abstractions will also use a stub ``uwsgi`` module when the real one is not available.

A couple of examples:

.. code-block:: python

    from uwsgiconf.runtime.locking import lock
    from uwsgiconf.runtime.scheduling import register_timer_rb

    @register_timer_rb(10, repeat=2)
    def repeat_twice():
        """This function will be called twice with 10 seconds interval
        using red-black tree based timer.

        """
        with lock():
            # Code under this context manager will be locked.
            do_something()


Allows for runtime access to:

* Alarms
* Caches
* Locks
* Logging
* Monitoring
* Mules
* RPC
* Scheduling
* Signals
* Websockets
* and more


Third parties support
~~~~~~~~~~~~~~~~~~~~~

**Django**

Run your Django-based project on uWSGI using manage command:

.. code-block:: bash

    $ ./manage.py uwsgi_run
    $ ./manage.py uwsgi_reload --force


* Other commands are available.
* uWSGI summary and statistics are also available from Admin interface.


System configs
~~~~~~~~~~~~~~

Compile system service config (e.g ``systemd``) to run your uWSGI-powered project:

.. code-block:: bash

    $ uwsgiconf sysinit systemd



Documentation
-------------

More information can be found at http://uwsgiconf.readthedocs.org/
