from django.conf import settings


MODULE_INIT_DEFAULT = 'uwsgiinit'
"""Default name for uwsgify init modules."""


MODULE_INIT = getattr(settings, 'UWSGIFY_MODULE_INIT', MODULE_INIT_DEFAULT)
"""Name of a module with uWSGI runtime related stuff 
to automatically import from registered applications.

"""
