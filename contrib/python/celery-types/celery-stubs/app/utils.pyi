from celery.utils.collections import ConfigurationView

class Settings(ConfigurationView): ...  # type: ignore[misc]  # pyright: ignore[reportImplicitAbstractClass]
