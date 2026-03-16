class Event(object):

    def __init__(self, event):
        self.key = event.kv.key
        self._event = event

    def __getattr__(self, name):
        if name.startswith('prev_'):
            return getattr(self._event.prev_kv, name[5:])
        return getattr(self._event.kv, name)

    def __str__(self):
        return '{type} key={key} value={value}'.format(type=self.__class__,
                                                       key=self.key,
                                                       value=self.value)


class PutEvent(Event):
    pass


class DeleteEvent(Event):
    pass


def new_event(event):
    """
    Wrap a raw gRPC event in a friendlier containing class.

    This picks the appropriate class from one of PutEvent or DeleteEvent and
    returns a new instance.
    """
    op_name = event.EventType.DESCRIPTOR.values_by_number[event.type].name
    if op_name == 'PUT':
        cls = PutEvent
    elif op_name == 'DELETE':
        cls = DeleteEvent
    else:
        raise Exception('Invalid op_name')

    return cls(event)
