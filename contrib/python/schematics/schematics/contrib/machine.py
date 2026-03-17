
import functools

from ..transforms import convert, to_primitive
from ..validate import validate


def _callback_wrap(data, schema, transform, *args, **kwargs):
    return transform(schema, data, *args, **kwargs)


class Machine(object):
    """ A poor man's state machine. """

    states = ('raw', 'converted', 'validated', 'serialized')
    transitions = (
        {'trigger': 'init', 'to': 'raw'},
        {'trigger': 'convert', 'from': 'raw', 'to': 'converted'},
        {'trigger': 'validate', 'from': 'converted', 'to': 'validated'},
        {'trigger': 'serialize', 'from': 'validated', 'to': 'serialized'}
    )
    callbacks = {
        'convert': functools.partial(_callback_wrap, transform=convert, partial=True),
        'validate': functools.partial(_callback_wrap, transform=validate, convert=False, partial=False),
        'serialize': functools.partial(_callback_wrap, transform=to_primitive)
    }

    def __init__(self, data, *args):
        self.state = self._transition(trigger='init')['to']
        self.data = data
        self.args = args

    def __getattr__(self, name):
        return functools.partial(self.trigger, name)

    def _transition(self, trigger=None, src_state=None, dst_state=None):
        try:
            return next(self._transitions(trigger=trigger, src_state=src_state,
            dst_state=dst_state))
        except StopIteration:
            return None

    def _transitions(self, trigger=None, src_state=None, dst_state=None):
        def pred(d, key, var):
            return d.get(key) == var if var is not None else True
        return (d for d in self.transitions if
            pred(d, 'trigger', trigger) and
            pred(d, 'from', src_state) and
            pred(d, 'to', dst_state)
        )

    def trigger(self, trigger):
        transition = self._transition(trigger=trigger, src_state=self.state)
        if not transition:
            raise AttributeError(trigger)
        callback = self.callbacks.get(trigger)
        self.data = callback(self.data, *self.args) if callback else self.data
        self.state = transition['to']

    def can(self, state):
        return bool(self._transition(src_state=self.state, dst_state=state))

    def cannot(self, state):
        return not self.can(state)
