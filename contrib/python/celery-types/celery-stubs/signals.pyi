from celery.utils.dispatch import Signal

__all__ = [
    "after_setup_logger",
    "after_setup_task_logger",
    "after_task_publish",
    "beat_embedded_init",
    "beat_init",
    "before_task_publish",
    "celeryd_after_setup",
    "celeryd_init",
    "eventlet_pool_apply",
    "eventlet_pool_postshutdown",
    "eventlet_pool_preshutdown",
    "eventlet_pool_started",
    "heartbeat_sent",
    "setup_logging",
    "task_failure",
    "task_internal_error",
    "task_postrun",
    "task_prerun",
    "task_retry",
    "task_revoked",
    "task_success",
    "worker_before_create_process",
    "worker_init",
    "worker_process_init",
    "worker_ready",
    "worker_shutdown",
    "worker_shutting_down",
]

before_task_publish: Signal
after_task_publish: Signal
task_received: Signal
task_prerun: Signal
task_postrun: Signal
task_success: Signal
task_retry: Signal
task_failure: Signal
task_revoked: Signal
task_rejected: Signal
task_internal_error: Signal
task_unknown: Signal
#: Deprecated, use after_task_publish instead.
task_sent: Signal

celeryd_init: Signal
celeryd_after_setup: Signal

import_modules: Signal
worker_init: Signal
worker_process_init: Signal
worker_process_shutdown: Signal
worker_ready: Signal
worker_shutdown: Signal
worker_shutting_down: Signal
worker_before_create_process: Signal
heartbeat_sent: Signal

setup_logging: Signal
after_setup_logger: Signal
after_setup_task_logger: Signal

beat_init: Signal
beat_embedded_init: Signal

eventlet_pool_started: Signal
eventlet_pool_preshutdown: Signal
eventlet_pool_postshutdown: Signal
eventlet_pool_apply: Signal

user_preload_options: Signal
