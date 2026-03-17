django-appconf
==============

.. image:: https://github.com/django-compressor/django-appconf/actions/workflows/tests.yml/badge.svg
    :alt: Build Status
    :target: https://github.com/django-compressor/django-appconf/actions/workflows/tests.yml

.. image:: https://badge.fury.io/py/django-appconf.svg
    :alt: PyPI version
    :target: https://pypi.org/project/django-appconf/

A helper class for handling configuration defaults of packaged Django
apps gracefully.

.. note::

    This app precedes Django's own AppConfig_ classes that act as
    "objects [to] store metadata for an application" inside Django's
    app loading mechanism. In other words, they solve a related but
    different use case than django-appconf and can't easily be used
    as a replacement. The similarity in name is purely coincidental.

.. _AppConfig: https://docs.djangoproject.com/en/stable/ref/applications/#django.apps.AppConfig

Overview
--------

Say you have an app called ``myapp`` with a few defaults, which you want
to refer to in the app's code without repeating yourself all the time.
``appconf`` provides a simple class to implement those defaults. Simply add
something like the following code somewhere in your app files:

.. code-block:: python

    from appconf import AppConf

    class MyAppConf(AppConf):
        SETTING_1 = "one"
        SETTING_2 = (
            "two",
        )

.. note::

    ``AppConf`` classes depend on being imported during startup of the Django
    process. Even though there are multiple modules loaded automatically,
    only the ``models`` modules (usually the ``models.py`` file of your
    app) are guaranteed to be loaded at startup. Therefore it's recommended
    to put your ``AppConf`` subclass(es) there, too.

The settings are initialized with the capitalized app label of where the
setting is located at. E.g. if your ``models.py`` with the ``AppConf`` class
is in the ``myapp`` package, the prefix of the settings will be ``MYAPP``.

You can override the default prefix by specifying a ``prefix`` attribute of
an inner ``Meta`` class:

.. code-block:: python

    from appconf import AppConf

    class AcmeAppConf(AppConf):
        SETTING_1 = "one"
        SETTING_2 = (
            "two",
        )

        class Meta:
            prefix = 'acme'

The ``MyAppConf`` class will automatically look at Django's global settings
to determine if you've overridden it. For example, adding this to your site's
``settings.py`` would override ``SETTING_1`` of the above ``MyAppConf``:

.. code-block:: python

    ACME_SETTING_1 = "uno"
    
Since django-appconf completes Django's global settings with its default values 
(like "one" above), the standard ``python manage.py diffsettings`` will show 
these defaults automatically.

In case you want to use a different settings object instead of the default
``'django.conf.settings'``, set the ``holder`` attribute of the inner
``Meta`` class to a dotted import path:

.. code-block:: python

    from appconf import AppConf

    class MyAppConf(AppConf):
        SETTING_1 = "one"
        SETTING_2 = (
            "two",
        )

        class Meta:
            prefix = 'acme'
            holder = 'acme.conf.settings'

If you ship an ``AppConf`` class with your reusable Django app, it's
recommended to put it in a ``conf.py`` file of your app package and
import ``django.conf.settings`` in it, too:

.. code-block:: python

    from django.conf import settings
    from appconf import AppConf

    class MyAppConf(AppConf):
        SETTING_1 = "one"
        SETTING_2 = (
            "two",
        )

In the other files of your app you can easily make sure the settings
are correctly loaded if you import Django's settings object from that
module, e.g. in your app's ``views.py``:

.. code-block:: python

    from django.http import HttpResponse
    from myapp.conf import settings

    def index(request):
        text = 'Setting 1 is: %s' % settings.MYAPP_SETTING_1
        return HttpResponse(text)

