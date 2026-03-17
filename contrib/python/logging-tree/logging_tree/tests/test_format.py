"""Tests for the `logging_tree.format` module."""

import doctest
import logging
import logging.handlers
import unittest
import sys

import logging_tree
from logging_tree.format import build_description, printout
from logging_tree.tests.case import LoggingTestCase
if sys.version_info >= (3,):
    from io import StringIO
else:
    from StringIO import StringIO


class FakeFile(StringIO):
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
        StringIO.__init__(self)

    def __repr__(self):
        return '<file %r>' % self.filename

    def fileno(self):
        return 0


class FormatTests(LoggingTestCase):

    maxDiff = 9999

    def setUp(self):
        # Prevent logging file handlers from trying to open real files.
        # (The keyword delay=1, which defers any actual attempt to open
        # a file, did not appear until Python 2.6.)
        logging.open = FakeFile
        super(FormatTests, self).setUp()

    def tearDown(self):
        del logging.open
        super(FormatTests, self).tearDown()

    def test_printout(self):
        stdout, sys.stdout = sys.stdout, StringIO()
        printout()
        self.assertEqual(sys.stdout.getvalue(), '<--""\n   Level WARNING\n')
        sys.stdout = stdout

    def test_simple_tree(self):
        logging.getLogger('a')
        logging.getLogger('a.b').setLevel(logging.DEBUG)
        logging.getLogger('x.c')
        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   |
   o<--"a"
   |   Level NOTSET so inherits level WARNING
   |   |
   |   o<--"a.b"
   |       Level DEBUG
   |
   o<--[x]
       |
       o<--"x.c"
           Level NOTSET so inherits level WARNING
''')

    def test_fancy_tree(self):
        logging.getLogger('').setLevel(logging.DEBUG)

        log = logging.getLogger('db')
        log.setLevel(logging.INFO)
        log.propagate = False
        log.disabled = 1
        log.addFilter(MyFilter())

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter())
        log.addHandler(handler)
        handler.addFilter(logging.Filter('db.errors'))

        logging.getLogger('db.errors')
        logging.getLogger('db.stats')

        log = logging.getLogger('www.status')
        log.setLevel(logging.DEBUG)
        log.addHandler(logging.FileHandler('/foo/log.txt'))
        log.addHandler(MyHandler())

        self.assertEqual(build_description(), '''\
<--""
   Level DEBUG
   |
   o   "db"
   |   Level INFO
   |   Propagate OFF
   |   Disabled
   |   Filter <MyFilter>
   |   Handler Stream %r
   |     Filter name='db.errors'
   |     Formatter fmt='%%(message)s' datefmt=None
   |   |
   |   o<--"db.errors"
   |   |   Level NOTSET so inherits level INFO
   |   |
   |   o<--"db.stats"
   |       Level NOTSET so inherits level INFO
   |
   o<--[www]
       |
       o<--"www.status"
           Level DEBUG
           Handler File '/foo/log.txt'
           Handler <MyHandler>
''' % (sys.stderr,))

    def test_most_handlers(self):
        ah = logging.getLogger('').addHandler
        ah(logging.handlers.RotatingFileHandler(
                '/bar/one.txt', maxBytes=10000, backupCount=3))
        ah(logging.handlers.SocketHandler('server.example.com', 514))
        ah(logging.handlers.DatagramHandler('server.example.com', 1958))
        ah(logging.handlers.SysLogHandler())
        ah(logging.handlers.SMTPHandler(
                'mail.example.com', 'Server', 'Sysadmin', 'Logs!'))
        # ah(logging.handlers.NTEventLogHandler())
        ah(logging.handlers.HTTPHandler('api.example.com', '/logs', 'POST'))
        ah(logging.handlers.BufferingHandler(20000))
        sh = logging.StreamHandler()
        ah(logging.handlers.MemoryHandler(30000, target=sh))
        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   Handler RotatingFile '/bar/one.txt' maxBytes=10000 backupCount=3
   Handler Socket server.example.com 514
   Handler Datagram server.example.com 1958
   Handler SysLog ('localhost', 514) facility=1
   Handler SMTP via mail.example.com to ['Sysadmin']
   Handler HTTP POST to http://api.example.com//logs
   Handler Buffering capacity=20000
   Handler Memory capacity=30000
     Flushes output to:
       Handler Stream %r
''' % (sh.stream,))
        logging.getLogger('').handlers[3].socket.close()  # or Python 3 warning

    def test_2_dot_5_handlers(self):
        if sys.version_info < (2, 5):
            return
        ah = logging.getLogger('').addHandler
        ah(logging.handlers.TimedRotatingFileHandler('/bar/two.txt'))
        expected = '''\
<--""
   Level WARNING
   Handler TimedRotatingFile '/bar/two.txt' when='H' interval=3600 backupCount=0
'''
        self.assertEqual(build_description(), expected)

    def test_2_dot_6_handlers(self):
        if sys.version_info < (2, 6):
            return
        ah = logging.getLogger('').addHandler
        ah(logging.handlers.WatchedFileHandler('/bar/three.txt'))
        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   Handler WatchedFile '/bar/three.txt'
''')

    def test_nested_handlers(self):
        h1 = logging.StreamHandler()

        h2 = logging.handlers.MemoryHandler(30000, target=h1)
        h2.addFilter(logging.Filter('worse'))
        h2.setLevel(logging.ERROR)

        h3 = logging.handlers.MemoryHandler(30000, target=h2)
        h3.addFilter(logging.Filter('bad'))

        logging.getLogger('').addHandler(h3)

        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   Handler Memory capacity=30000
     Filter name='bad'
     Flushes output to:
       Handler Memory capacity=30000
         Level ERROR
         Filter name='worse'
         Flushes output to:
           Handler Stream %r
''' % (h1.stream,))

    def test_formatter_with_no_fmt_attributes(self):
        f = logging.Formatter()
        del f._fmt
        del f.datefmt
        h = logging.StreamHandler()
        h.setFormatter(f)
        logging.getLogger('').addHandler(h)

        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   Handler Stream %r
     Formatter fmt=None datefmt=None
''' % (h.stream,))

    def test_formatter_that_is_not_a_Formatter_instance(self):
        h = logging.StreamHandler()
        h.setFormatter("Ceci n'est pas une formatter")
        logging.getLogger('').addHandler(h)

        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   Handler Stream %r
     Formatter "Ceci n'est pas une formatter"
''' % (h.stream,))

    def test_handler_with_wrong_parent_attribute(self):
        logging.getLogger('celery')
        logging.getLogger('app.model')
        logging.getLogger('app.task').parent = logging.getLogger('celery.task')
        logging.getLogger('app.view')

        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   |
   o<--[app]
   |   |
   |   o<--"app.model"
   |   |   Level NOTSET so inherits level WARNING
   |   |
   |   o !-"app.task"
   |   |   Broken .parent redirects messages to 'celery.task' instead
   |   |   Level NOTSET so inherits level WARNING
   |   |
   |   o<--"app.view"
   |       Level NOTSET so inherits level WARNING
   |
   o<--"celery"
       Level NOTSET so inherits level WARNING
       |
       o<--"celery.task"
           Level NOTSET so inherits level WARNING
''')

    def test_handler_with_parent_attribute_that_is_none(self):
        logging.getLogger('app').parent = None

        self.assertEqual(build_description(), '''\
<--""
   Level WARNING
   |
   o !-"app"
       Broken .parent is None, so messages stop here
       Level NOTSET so inherits level NOTSET
''')


class MyFilter(object):
    def __repr__(self):
        return '<MyFilter>'


class MyHandler(object):
    def __repr__(self):
        return '<MyHandler>'


def _spoofout_repr(self):
    """Improve how doctest's fake stdout looks in our package's doctest."""
    return '<sys.stdout>'

doctest._SpoofOut.__repr__ = _spoofout_repr

def load_tests(loader, suite, ignore):
    suite.addTests(doctest.DocTestSuite(logging_tree))
    return suite

if __name__ == '__main__':  # for Python <= 2.4
    unittest.main()
