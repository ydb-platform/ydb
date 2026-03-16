# coding: utf-8

# DJANGO IMPORTS
from django.apps import AppConfig


class GrappelliConfig(AppConfig):
    name = 'grappelli'

    def ready(self):
        from .checks import register_checks
        register_checks()
