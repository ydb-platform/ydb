"""
URLpatterns for the debug panel.

These should not be loaded explicitly; It is used internally by the
debug-panel application.
"""
from .views import debug_data

try:
    from django.conf.urls import url
except ImportError:  # django < 1.4
    from django.conf.urls.defaults import url

_PREFIX = '__debug__'

urlpatterns = [
    url(r'^%s/data/(?P<cache_key>\d+\.\d+)/$' % _PREFIX, debug_data, name='debug_data'),
]
