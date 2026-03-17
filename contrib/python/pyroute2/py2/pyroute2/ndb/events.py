import time


class SyncStart(Exception):
    pass


class SchemaFlush(Exception):
    pass


class SchemaReadLock(Exception):
    pass


class SchemaReadUnlock(Exception):
    pass


class SchemaGenericRequest(object):
    def __init__(self, response, *argv, **kwarg):
        self.response = response
        self.argv = argv
        self.kwarg = kwarg


class MarkFailed(Exception):
    pass


class DBMExitException(Exception):
    pass


class ShutdownException(Exception):
    pass


class RescheduleException(Exception):
    pass


class InvalidateHandlerException(Exception):
    pass


class State(object):

    events = None

    def __init__(self, prime=None, log=None):
        self.events = []
        self.log = log
        if prime is not None:
            self.load(prime)

    def load(self, prime):
        self.events = []
        for state in prime.events:
            self.events.append(state)

    def transition(self):
        if len(self.events) < 2:
            return None
        return (self.events[-2][1], self.events[-1][1])

    def get(self):
        if not self.events:
            return None
        return self.events[-1][1]

    def set(self, state):
        if self.log is not None:
            self.log.debug(state)
        if self.events and self.events[-1][1] == state:
            self.events.pop()
        self.events.append((time.time(), state))
        return state

    def __eq__(self, other):
        if not self.events:
            return False
        return self.events[-1][1] == other

    def __ne__(self, other):
        if not self.events:
            return True
        return self.events[-1][1] != other
