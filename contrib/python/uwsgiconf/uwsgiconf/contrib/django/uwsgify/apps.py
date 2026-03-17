from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules
from django.utils.translation import gettext_lazy as _

from .settings import MODULE_INIT_DEFAULT


class UwsgifyConfig(AppConfig):

    name = 'uwsgiconf.contrib.django.uwsgify'
    verbose_name = _('uWSGI Integration')

    def ready(self):

        try:
            import uwsgi

            # This will handle init modules discovery for non-embedded.
            # Part for embedding is done in toolbox.SectionMutator.mutate
            # via in master process import.
            autodiscover_modules(MODULE_INIT_DEFAULT)

        except ImportError:
            pass
