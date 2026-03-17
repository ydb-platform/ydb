from typing import Any

from django.db.models.signals import pre_save
from django.dispatch import receiver

from .models import DBTaskResult, get_date_max


@receiver(pre_save, sender=DBTaskResult)
def set_run_after(sender: Any, instance: DBTaskResult, **kwargs: Any) -> None:
    if instance.run_after is None:
        instance.run_after = get_date_max()
