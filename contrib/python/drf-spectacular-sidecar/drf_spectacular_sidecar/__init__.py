import django

__version__ = '2026.1.1'

if django.VERSION < (3, 2):
    default_app_config = 'drf_spectacular_sidecar.apps.SpectacularSidecarConfig'
