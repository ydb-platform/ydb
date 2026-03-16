# -*- coding: utf-8 -*-

__version__ = '0.7.0'

default_app_config = "moderation.apps.ModerationConfig"


class _ModerationProxy:
    """Proxy the ModerationManager()

    It must not be created here for Django >= 1.8, because __init__ is invoked
    too early, but must be present once a module registers its models.
    """
    _moderation = None

    def _ensure_obj(self):
        if _ModerationProxy._moderation is None:
            from .register import ModerationManager
            _ModerationProxy._moderation = ModerationManager()

    def __getattr__(self, attribute):
        self._ensure_obj()
        return getattr(_ModerationProxy._moderation, attribute)

    def __setattr__(self, attribute, value):
        self._ensure_obj()
        return setattr(_ModerationProxy._moderation, attribute, value)


moderation = _ModerationProxy()
