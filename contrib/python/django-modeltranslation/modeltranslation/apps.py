# -*- coding: utf-8 -*-
from django.apps import AppConfig


class ModeltranslationConfig(AppConfig):
    name = 'modeltranslation'
    verbose_name = 'Modeltranslation'

    def ready(self):
        from modeltranslation.models import handle_translation_registrations

        handle_translation_registrations()
