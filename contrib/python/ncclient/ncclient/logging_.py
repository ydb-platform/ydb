import logging

class SessionLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that automatically adds session information to logs."""

    def process(self, msg, kwargs):
        if 'session' not in self.extra or self.extra['session'] is None:
            return msg, kwargs
        session = self.extra['session']
        prefix = ""
        # All Session instances have an id. SSHSessions have a host as well.
        if hasattr(session, 'host'):
            prefix += "host %s " % session.host

        if session.id is not None:
            prefix += "session-id %s" % session.id
        else:
            prefix += "session 0x%x" % id(session)

        # Pass the session information through to the LogRecord itself
        if 'extra' not in kwargs:
            kwargs['extra'] = self.extra
        else:
            kwargs['extra'].update(self.extra)

        return "[%s] %s" % (prefix, msg), kwargs
