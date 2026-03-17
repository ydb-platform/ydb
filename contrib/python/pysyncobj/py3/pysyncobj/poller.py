import select


class POLL_EVENT_TYPE:
    READ = 1
    WRITE = 2
    ERROR = 4


class Poller(object):
    def subscribe(self, descr, callback, eventMask):
        raise NotImplementedError

    def unsubscribe(self, descr):
        raise NotImplementedError

    def poll(self, timeout):
        raise NotImplementedError


class SelectPoller(Poller):
    def __init__(self):
        self.__descrsRead = set()
        self.__descrsWrite = set()
        self.__descrsError = set()
        self.__descrToCallbacks = {}

    def subscribe(self, descr, callback, eventMask):
        self.unsubscribe(descr)
        if eventMask & POLL_EVENT_TYPE.READ:
            self.__descrsRead.add(descr)
        if eventMask & POLL_EVENT_TYPE.WRITE:
            self.__descrsWrite.add(descr)
        if eventMask & POLL_EVENT_TYPE.ERROR:
            self.__descrsError.add(descr)
        self.__descrToCallbacks[descr] = callback

    def unsubscribe(self, descr):
        self.__descrsRead.discard(descr)
        self.__descrsWrite.discard(descr)
        self.__descrsError.discard(descr)
        self.__descrToCallbacks.pop(descr, None)

    def poll(self, timeout):
        rlist, wlist, xlist = select.select(list(self.__descrsRead),
                                            list(self.__descrsWrite),
                                            list(self.__descrsError),
                                            timeout)

        allDescrs = set(rlist + wlist + xlist)
        rlist = set(rlist)
        wlist = set(wlist)
        xlist = set(xlist)
        for descr in allDescrs:
            event = 0
            if descr in rlist:
                event |= POLL_EVENT_TYPE.READ
            if descr in wlist:
                event |= POLL_EVENT_TYPE.WRITE
            if descr in xlist:
                event |= POLL_EVENT_TYPE.ERROR
            self.__descrToCallbacks[descr](descr, event)


class PollPoller(Poller):
    def __init__(self):
        self.__poll = select.poll()
        self.__descrToCallbacks = {}

    def subscribe(self, descr, callback, eventMask):
        pollEventMask = 0
        if eventMask & POLL_EVENT_TYPE.READ:
            pollEventMask |= select.POLLIN
        if eventMask & POLL_EVENT_TYPE.WRITE:
            pollEventMask |= select.POLLOUT
        if eventMask & POLL_EVENT_TYPE.ERROR:
            pollEventMask |= select.POLLERR
        self.__descrToCallbacks[descr] = callback
        self.__poll.register(descr, pollEventMask)

    def unsubscribe(self, descr):
        try:
            self.__poll.unregister(descr)
        except KeyError:
            pass

    def poll(self, timeout):
        events = self.__poll.poll(timeout * 1000)
        for descr, event in events:
            eventMask = 0
            if event & select.POLLIN:
                eventMask |= POLL_EVENT_TYPE.READ
            if event & select.POLLOUT:
                eventMask |= POLL_EVENT_TYPE.WRITE
            if event & select.POLLERR or event & select.POLLHUP:
                eventMask |= POLL_EVENT_TYPE.ERROR
            self.__descrToCallbacks[descr](descr, eventMask)


def createPoller(pollerType):
    if pollerType == 'auto':
        if hasattr(select, 'poll'):
            return PollPoller()
        return SelectPoller()
    elif pollerType == 'poll':
        return PollPoller()
    elif pollerType == 'select':
        return SelectPoller()
    else:
        raise Exception('unknown poller type')
