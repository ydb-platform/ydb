from celery.worker.components import Hub, Pool, Timer

class WorkController:
    hub: Hub | None
    pool: Pool | None
    timer: Timer | None
