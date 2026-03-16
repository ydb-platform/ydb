import asyncio
import collections
import copy
import logging
from typing import ClassVar

from aiozk import exc

from .iterables import drain


log = logging.getLogger(__name__)


class States:
    CONNECTED = 'connected'
    SUSPENDED = 'suspended'
    READ_ONLY = 'read_only'
    LOST = 'lost'


class SessionStateMachine:
    valid_transitions: ClassVar = {
        (States.LOST, States.CONNECTED),
        (States.LOST, States.READ_ONLY),
        (States.CONNECTED, States.SUSPENDED),
        (States.CONNECTED, States.LOST),
        (States.READ_ONLY, States.CONNECTED),
        (States.READ_ONLY, States.SUSPENDED),
        (States.READ_ONLY, States.LOST),
        (States.SUSPENDED, States.CONNECTED),
        (States.SUSPENDED, States.READ_ONLY),
        (States.SUSPENDED, States.LOST),
    }

    def __init__(self, session):
        self.session = session
        self.current_state = States.LOST
        self.futures = collections.defaultdict(set)

    def transition_to(self, state):
        if (self.current_state, state) not in self.valid_transitions:
            raise exc.InvalidStateTransition('Invalid session state transition: %s -> %s' % (self.current_state, state))

        log.debug('Session transition: %s -> %s', self.current_state, state)

        self.current_state = state

        for future in drain(self.futures[state]):
            if not future.done():
                future.set_result(None)

    def wait_for(self, *states):
        loop = asyncio.get_running_loop()
        f = loop.create_future()

        if self.current_state in states:
            f.set_result(None)
        else:
            for state in states:
                self.futures[state].add(f)

        return f

    def remove_waiting(self, future, *states):
        for state in states:
            self.futures[state].remove(future)

    def waitings(self, *states):
        futures = {}
        for state in states:
            futures[state] = copy.copy(self.futures[state])

        return futures

    def __eq__(self, state):
        return self.current_state == state

    def __ne__(self, state):
        return self.current_state != state

    def __str__(self):
        return '<SessionStateMachine {}>'.format(self.current_state)
