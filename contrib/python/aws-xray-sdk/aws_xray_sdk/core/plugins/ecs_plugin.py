import socket
import logging

log = logging.getLogger(__name__)

SERVICE_NAME = 'ecs'
ORIGIN = 'AWS::ECS::Container'


def initialize():
    global runtime_context
    try:
        runtime_context = {}
        host_name = socket.gethostname()
        if host_name:
            runtime_context['container'] = host_name

    except Exception:
        runtime_context = None
        log.warning("failed to get ecs container metadata")
