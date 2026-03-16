'''Autoconfig data for applications that don't support this protocol.'''

from collections import namedtuple
import django
from django_autoconfig.autoconfig import OrderingRelationship

Autoconfig = namedtuple('Autoconfig', ('SETTINGS', 'DEFAULT_SETTINGS', 'RELATIONSHIPS'))
if django.VERSION < (1, 10):
    middleware_key = 'MIDDLEWARE_CLASSES'
else:
    middleware_key = 'MIDDLEWARE'

CONTRIB_CONFIGS = {
    'django.contrib.auth': Autoconfig(
        SETTINGS = {
            'INSTALLED_APPS': [
                'django.contrib.contenttypes',
                'django.contrib.sessions',
            ],
            middleware_key: [
                'django.contrib.auth.middleware.AuthenticationMiddleware',
            ],
        },
        DEFAULT_SETTINGS = {},
        RELATIONSHIPS = [
            OrderingRelationship(
                middleware_key,
                'django.contrib.auth.middleware.AuthenticationMiddleware',
                after = [
                    'django.middleware.common.CommonMiddleware',
                    'django.contrib.sessions.middleware.SessionMiddleware',
                    'django.middleware.csrf.CsrfViewMiddleware',
                ],
                add_missing = False,
            )
        ],
    ),
    'django.contrib.messages': Autoconfig(
        SETTINGS = {
            'INSTALLED_APPS': [
                'django.contrib.sessions',
            ],
            middleware_key: [
                'django.contrib.messages.middleware.MessageMiddleware',
            ],
            'TEMPLATE_CONTEXT_PROCESSORS': [
                'django.contrib.messages.context_processors.messages',
            ],
            'TEMPLATES': [
                {
                    'BACKEND': 'django.template.backends.django.DjangoTemplates',
                    'OPTIONS': {
                        'context_processors': [
                            'django.contrib.messages.context_processors.messages',
                        ],
                    },
                },
            ],
        },
        DEFAULT_SETTINGS = {},
        RELATIONSHIPS = [
            OrderingRelationship(
                middleware_key,
                'django.contrib.messages.middleware.MessageMiddleware',
                after = [
                    'django.contrib.sessions.middleware.SessionMiddleware',
                    'django.contrib.auth.middleware.AuthenticationMiddleware',
                ],
                add_missing = False,
            )
        ],
    ),
    'django.contrib.sessions': Autoconfig(
        SETTINGS = {
            middleware_key: [
                'django.contrib.sessions.middleware.SessionMiddleware',
            ],
        },
        DEFAULT_SETTINGS = {},
        RELATIONSHIPS = [
            OrderingRelationship(
                middleware_key,
                'django.contrib.sessions.middleware.SessionMiddleware',
                after = [
                    'django.middleware.cache.UpdateCacheMiddleware',
                ],
                before = [
                    'django.middleware.common.CommonMiddleware',
                ],
                add_missing = False,
            )
        ],
    ),
    'django.contrib.admin': Autoconfig(
        SETTINGS = {
            'INSTALLED_APPS': [
                'django.contrib.auth',
                'django.contrib.contenttypes',
                'django.contrib.messages',
                'django.contrib.staticfiles',
            ],
            'AUTOCONFIG_EXTRA_URLS': [
                'django_autoconfig.contrib.admin',
            ],
            'TEMPLATE_CONTEXT_PROCESSORS': [
                'django.contrib.auth.context_processors.auth',
            ],
            'TEMPLATES': [
                {
                    'BACKEND': 'django.template.backends.django.DjangoTemplates',
                    'OPTIONS': {
                        'context_processors': [
                            'django.contrib.auth.context_processors.auth',
                        ],
                    },
                },
            ],
        },
        DEFAULT_SETTINGS = {},
        RELATIONSHIPS = [
            OrderingRelationship(
                'INSTALLED_APPS',
                'django.contrib.admin',
                after = [
                    'django.contrib.contenttypes',
                ],
                add_missing = False,
            )
        ],
    ),
}
