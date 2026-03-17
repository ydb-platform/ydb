# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function

import os
import sys

from library.python.django.utils import patch_settings_for_arcadia


PROJECT_PATH = os.path.join(os.getenv('ARCADIA_PATH'), 'library/python/django/example/django_rest_api/')

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(PROJECT_PATH, 'polls.db'),
    },
}


DEBUG = True

ROOT_URLCONF = 'library.python.django.example.django_rest_api.polls.urls'

REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 10,
}

SECRET_KEY = 'foo'
STATIC_URL = '/static/'
STATIC_ROOT = 'static'
STATICFILES_FINDERS = [
    'library.python.django.contrib.staticfiles.finders.ArcadiaAppFinder',
]

STATICFILES_DIRS = [
    os.path.join(PROJECT_PATH, 'static'),
]

FIXTURE_DIRS = ['common_fixtures']

TEMPLATES = [
    {
        'BACKEND': 'library.python.django.template.backends.arcadia.ArcadiaTemplates',
        'OPTIONS': {
            'debug': True,
            'loaders': [
                'library.python.django.template.loaders.resource.Loader',
                'library.python.django.template.loaders.app_resource.Loader',
            ],
        },
    },
]

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.staticfiles',
    'django.contrib.contenttypes',
    'django.contrib.sites',
    'library.python.django.example.django_rest_api.polls',
    'rest_framework',
]

LOG_FORMAT = os.environ.get('QLOUD_LOGGER_STDOUT_PARSER', 'default')

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            '()': 'ylog.context.ContextFormatter',
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'format': '[%(asctime)s][%(name)s] - %(levelname)s - %(message)s',
        },
        'json': {
            '()': 'ylog.format.QloudJsonFormatter',
        },
    },
    'handlers': {
        'stdout': {
            'class': 'logging.StreamHandler',
            'formatter': LOG_FORMAT,
            'stream': sys.stdout,
        },
        'stderr': {
            'class': 'logging.StreamHandler',
            'formatter': LOG_FORMAT,
            'stream': sys.stderr,
        },
    },
    'loggers': {
        '': {
            'handlers': ['stderr'],
            'propagate': False,
            'level': 'WARNING',
        },
        'requests': {
            'handlers': ['stdout'],
            'propagate': False,
            'level': 'INFO',
        },
        'django': {
            'handlers': ['stdout'],
            'propagate': False,
            'level': 'WARNING',
        },
    },
    'root': {
        'handlers': ['stdout'],
        'level': 'INFO',
    },
}

patch_settings_for_arcadia()
