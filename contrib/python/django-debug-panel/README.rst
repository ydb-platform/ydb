==================
Django Debug Panel
==================

Django Debug Toolbar inside WebKit DevTools. Works fine with background AJAX requests and non-HTML responses.
Great for single-page applications and other AJAX intensive web applications.

Installation
============

#. Install and configure `Django Debug Toolbar <https://github.com/django-debug-toolbar/django-debug-toolbar>`_

#. Install Django Debug Panel:

   .. code-block:: bash

    pip install django-debug-panel

#. Add ``debug_panel`` to your ``INSTALLED_APPS`` setting:

   .. code-block:: python

    INSTALLED_APPS = (
        # ...
        'debug_panel',
    )

#. Replace the Django Debug Toolbar middleware with the Django Debug Panel one. Replace:

   .. code-block:: python

    MIDDLEWARE_CLASSES = (
        ...
        'debug_toolbar.middleware.DebugToolbarMiddleware',
        ...
    )

   with:

   .. code-block:: python

    MIDDLEWARE_CLASSES = (
        ...
        'debug_panel.middleware.DebugPanelMiddleware',
        ...
    )


#. (Optional) Configure your cache.
   All the debug data of a request are stored into the cache backend ``debug-panel``
   if available. Otherwise, the ``default`` backend is used, and finally if no caches are
   defined it will fallback to a local memory cache.
   You might want to configure the ``debug-panel`` cache in your ``settings``:

   .. code-block:: python

    CACHES = {
        'default': {
            'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
            'LOCATION': '127.0.0.1:11211',
        },

        # this cache backend will be used by django-debug-panel
        'debug-panel': {
            'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
            'LOCATION': '/var/tmp/debug-panel-cache',
            'OPTIONS': {
                'MAX_ENTRIES': 200
            }
        }
    }

#. Install the Chrome extension `Django Debug Panel <https://chrome.google.com/webstore/detail/django-debug-panel/nbiajhhibgfgkjegbnflpdccejocmbbn>`_
