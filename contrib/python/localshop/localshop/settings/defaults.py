import importlib
import os
import sys
import uuid

import environ
from celery.schedules import crontab
from django.contrib import messages


env = environ.Env()

# Django settings for localshop project.
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

DEBUG = env.bool('DEBUG', default=False)

DATABASES = {
    'default': env.db(default='sqlite:///localshop.db'),
}

# Make this unique, and don't share it with anybody.
SECRET_KEY = env.str('SECRET_KEY', default=uuid.uuid4())

# Local time zone for this installation. Choices can be found here:
TIME_ZONE = env.str('TIME_ZONE', default='UTC')
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

USE_I18N = True
USE_L10N = True
USE_TZ = True

MEDIA_ROOT = env.str('LOCALSHOP_ROOT', os.path.join(PROJECT_ROOT, 'public', 'media'))

# Staticfiles
STATIC_ROOT = env.str('STATIC_ROOT', os.path.join(PROJECT_ROOT, 'public', 'static'))
STATIC_URL = '/static/'
STATICFILES_DIRS = [
    os.path.join(PROJECT_ROOT, 'static')
]
STATICFILES_STORAGE = 'django.contrib.staticfiles.storage.ManifestStaticFilesStorage'
STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
]

SESSION_COOKIE_AGE = 28 * 24 * 60 * 60  # 4 weeks

MIDDLEWARE = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
)

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(PROJECT_ROOT, 'templates'),
        ],
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.template.context_processors.static',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
            'loaders': [
                ('django.template.loaders.cached.Loader', [
                    'django.template.loaders.filesystem.Loader',
                    'django.template.loaders.app_directories.Loader',
                ]),
            ],
        },
    },
]

ROOT_URLCONF = 'localshop.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'localshop.wsgi.application'

MESSAGE_TAGS = {
    messages.ERROR: 'danger'
}

BROKER_URL = env.str('BROKER_URL', default='redis://127.0.0.1:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_DEFAULT_QUEUE = 'default'
CELERYD_PREFETCH_MULTIPLIER = 0
CELERY_RESULT_BACKEND = 'django-db'
CELERYBEAT_SCHEDULER = 'django_celery_beat.schedulers.DatabaseScheduler'
CELERYBEAT_SCHEDULE = {
    'refresh-repos': {
        'task': 'localshop.apps.packages.tasks.refresh_repository_mirrors',
        'schedule': crontab(minute=30),
    },
}
CELERY_IMPORTS = [
    'localshop.apps.packages.tasks',
]

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.admin',
    'django.contrib.humanize',

    'django_celery_beat',
    'django_celery_results',
    'widget_tweaks',

    'localshop',
    'localshop.apps.accounts',
    'localshop.apps.dashboard',
    'localshop.apps.packages',
    'localshop.apps.permissions',
]

# Auth settings
AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',
]
LOGIN_URL = '/accounts/login'
LOGIN_REDIRECT_URL = '/dashboard/'
LOGOUT_URL = '/accounts/logout'
AUTH_USER_MODEL = 'accounts.User'

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'root': {
        'handlers': ['console'],
        'propagate': True,
        'level': 'DEBUG',
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler'
        },
    },
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
    },
}

EMAIL = env.email_url('EMAIL', 'smtp://localhost:25/')

ALLOWED_HOSTS = ['*']

DATA_UPLOAD_MAX_MEMORY_SIZE = 100 * 1024 * 1024  # 100 mb
DEFAULT_FILE_STORAGE = env.str(
    'LOCALSHOP_FILE_STORAGE',
    default='django.core.files.storage.FileSystemStorage')

AWS_STORAGE_BUCKET_NAME = env.str('LOCALSHOP_FILE_BUCKET_NAME', default='')

LOCALSHOP_DELETE_FILES = False
LOCALSHOP_HTTP_PROXY = None
LOCALSHOP_ISOLATED = False
LOCALSHOP_RELEASE_OVERWRITE = True

# Use X-Forwarded-For header as the source for the client's IP.
# Use where you have Nginx/Apache/etc as a reverse proxy infront of Localshop/Gunicorn.
LOCALSHOP_USE_PROXIED_IP = False
LOCALSHOP_VERSIONING_TYPE = None
LOCALSHOP_VERSION_VALIDATION = False

# Load the user settings
filename = os.path.expanduser('~/conf/localshop.conf.py')
spec = importlib.util.spec_from_file_location('localshop.settings.user', filename)
user_settings = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(user_settings)
except FileNotFoundError:
    pass
else:
    for key, value in user_settings.__dict__.items():
        if key.startswith('_'):
            continue
        setattr(sys.modules[__name__], key, value)

QUARANTINE_THRESHOLD = 7  # days
