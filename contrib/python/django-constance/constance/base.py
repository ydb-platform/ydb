from . import settings, utils


class Config:
    """
    The global config wrapper that handles the backend.
    """
    def __init__(self):
        super().__setattr__('_backend',
            utils.import_module_attr(settings.BACKEND)())

    def __getattr__(self, key):
        try:
            if not len(settings.CONFIG[key]) in (2, 3):
                raise AttributeError(key)
            default = settings.CONFIG[key][0]
        except KeyError:
            raise AttributeError(key)
        result = self._backend.get(key)
        if result is None:
            result = default
            setattr(self, key, default)
            return result
        return result

    def __setattr__(self, key, value):
        if key not in settings.CONFIG:
            raise AttributeError(key)
        self._backend.set(key, value)

    def __dir__(self):
        return settings.CONFIG.keys()
