from django.apps import AppConfig
from django.core import checks
from django.utils.translation import gettext_lazy as _

from .checks import check_default_host, check_root_hostconf


class HostsConfig(AppConfig):  # pragma: no cover
    name = 'django_hosts'
    verbose_name = _('Hosts')

    def ready(self):
        checks.register(check_root_hostconf)
        checks.register(check_default_host)
