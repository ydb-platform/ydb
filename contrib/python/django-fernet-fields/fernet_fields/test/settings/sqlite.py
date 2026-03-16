from .base import *  # noqa

DB = 'mydatabase'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': DB,
        'TEST': {
            'NAME': DB,
        },
    },
}
