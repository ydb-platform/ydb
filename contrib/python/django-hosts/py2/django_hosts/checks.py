from django.conf import settings
from django.core import checks

E001 = checks.Error(
    "Missing 'DEFAULT_HOST' setting.",
    hint="Has to be the name of the default host pattern.",
    id='django_hosts.E001',
)

E002 = checks.Error(
    "Missing 'ROOT_HOSTCONF' setting.",
    hint="Has to be the dotted Python import path of "
         "the module containing your host patterns.",
    id='django_hosts.E002',
)


def check_default_host(app_configs, **kwargs):  # pragma: no cover
    return [] if getattr(settings, 'DEFAULT_HOST', False) else [E001]


def check_root_hostconf(app_configs, **kwargs):  # pragma: no cover
    return [] if getattr(settings, 'ROOT_HOSTCONF', False) else [E002]
