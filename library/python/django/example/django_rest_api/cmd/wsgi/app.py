# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function
import os

from django.core.wsgi import get_wsgi_application
from library.python.gunicorn import run_standalone

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "library.python.django.example.django_rest_api.settings")
application = get_wsgi_application()


def main():
    run_standalone(application)
