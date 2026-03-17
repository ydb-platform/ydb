from django.apps import AppConfig, apps
from django.db.models.signals import post_delete


class OrderedModelConfig(AppConfig):
    name = "ordered_model"
    label = "ordered_model"

    def ready(self):
        from .models import OrderedModelBase

        for cls in apps.get_models():
            if issubclass(cls, OrderedModelBase):
                post_delete.connect(
                    cls._on_ordered_model_delete, sender=cls, dispatch_uid=cls.__name__
                )
