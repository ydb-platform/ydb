# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.apps import AppConfig
from django.conf import settings
from django.core.signals import request_started
from .state import reset_state


class DjangoToolsLogContextConfig(AppConfig):
    name = 'django_tools_log_context'

    def ready(self):
        from .tracking import enable_tracking
        request_started.connect(reset_state)
        if settings.TOOLS_LOG_CONTEXT_ENABLE_TRACKING:
            enable_tracking()
