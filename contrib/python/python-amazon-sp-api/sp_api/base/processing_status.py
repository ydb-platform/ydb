from enum import Enum


class ProcessingStatus(str, Enum):
    CANCELLED = 'CANCELLED'
    DONE = 'DONE'
    FATAL = 'FATAL'
    IN_PROGRESS = 'IN_PROGRESS'
    IN_QUEUE = 'IN_QUEUE'
