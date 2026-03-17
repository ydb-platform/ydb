from typing import Literal

from celery import shared_task

@shared_task(name="celery.ping")
def ping() -> Literal["pong"]: ...
