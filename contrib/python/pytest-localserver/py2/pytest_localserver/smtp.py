#!/usr/bin/env python
# -*- coding: utf8 -*-
#
# Copyright (C) 2011 Sebastian Rahlf <basti at redtoad dot de>
# with some ideas from http://code.activestate.com/recipes/440690/
# SmtpMailsink Copyright 2005 Aviarc Corporation
# Written by Adam Feuer, Matt Branthwaite, and Troy Frever
# which is Licensed under the PSF License

import asyncore
import email
import smtpd
import sys
import threading


PY35_OR_NEWER = sys.version_info[:2] >= (3, 5)

class Server (smtpd.SMTPServer, threading.Thread):

    """
    Small SMTP test server. Try the following snippet for sending mail::

        server = Server(port=8080)
        server.start()
        print 'SMTP server is running on %s:%i' % server.addr

        # any e-mail sent to localhost:8080 will end up in server.outbox
        # ...

        server.stop()

    """

    WAIT_BETWEEN_CHECKS = 0.001

    def __init__(self, host='localhost', port=0):
        # Workaround for deprecated signature in Python 3.6
        if PY35_OR_NEWER:
            smtpd.SMTPServer.__init__(self, (host, port), None, decode_data=True)
        else:
            smtpd.SMTPServer.__init__(self, (host, port), None)

        if self._localaddr[1] == 0:
            self.addr = self.socket.getsockname()

        self.outbox = []

        # initialise thread
        self._stopevent = threading.Event()
        self.threadName = self.__class__.__name__
        threading.Thread.__init__(self, name=self.threadName)

    def process_message(self, peer, mailfrom, rcpttos, data, **kwargs):
        """
        Adds message to outbox.
        """
        try:
            message = email.message_from_bytes(data)
        except AttributeError:
            message = email.message_from_string(data)
        # on the message, also set the envelope details

        class Bunch:
            def __init__(self, **kwds):
                vars(self).update(kwds)

        message.details = Bunch(
            peer=peer,
            mailfrom=mailfrom,
            rcpttos=rcpttos,
            **kwargs
        )
        self.outbox.append(message)

    def run(self):
        """
        Threads run method.
        """
        while not self._stopevent.is_set():
            asyncore.loop(timeout=self.WAIT_BETWEEN_CHECKS, count=1)

    def stop(self, timeout=None):
        """
        Stops test server.

        :param timeout: When the timeout argument is present and not None, it
        should be a floating point number specifying a timeout for the
        operation in seconds (or fractions thereof).
        """
        self._stopevent.set()
        threading.Thread.join(self, timeout)
        self.close()

    def __del__(self):
        self.stop()

    def __repr__(self):  # pragma: no cover
        return '<smtp.Server %s:%s>' % self.addr

if __name__ == "__main__":  # pragma: no cover
    import time

    server = Server()
    server.start()

    print('SMTP server is running on %s:%i' % server.addr)
    print('Type <Ctrl-C> to stop')

    try:

        try:
            while True:
                time.sleep(1)
        finally:
            print('\rstopping...')
            server.stop()

    except KeyboardInterrupt:
        # support for Python 2.4 dictates that try ... finally is not used
        # together with any except statements
        pass
