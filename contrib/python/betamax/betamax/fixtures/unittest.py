"""Minimal :class:`unittest.TestCase` subclass adding Betamax integration.

.. autoclass:: betamax.fixtures.unittest.BetamaxTestCase
    :members:

When using Betamax with unittest, you can use the traditional style of Betamax
covered in the documentation thoroughly, or you can use your fixture methods,
:meth:`unittest.TestCase.setUp` and :meth:`unittest.TestCase.tearDown` to wrap
entire tests in Betamax.

Here's how you might use it:

.. code-block:: python

    from betamax.fixtures import unittest

    from myapi import SessionManager


    class TestMyApi(unittest.BetamaxTestCase):
        def setUp(self):
            # Call BetamaxTestCase's setUp first to get a session
            super(TestMyApi, self).setUp()

            self.manager = SessionManager(self.session)

        def test_all_users(self):
            \"\"\"Retrieve all users from the API.\"\"\"
            for user in self.manager:
                # Make assertions or something

Alternatively, if you are subclassing a :class:`requests.Session` to provide
extra functionality, you can do something like this:

.. code-block:: python

    from betamax.fixtures import unittest

    from myapi import Session, SessionManager


    class TestMyApi(unittest.BetamaxTestCase):
        SESSION_CLASS = Session

        # See above ...

"""
# NOTE(sigmavirus24): absolute_import is required to make import unittest work
from __future__ import absolute_import

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import requests

from .. import recorder


__all__ = ('BetamaxTestCase',)


class BetamaxTestCase(unittest.TestCase):

    """Betamax integration for unittest.

    .. versionadded:: 0.5.0
    """

    #: Class that is a subclass of :class:`requests.Session`
    SESSION_CLASS = requests.Session

    #: Custom path to save cassette.
    CASSETTE_LIBRARY_DIR = None

    def generate_cassette_name(self):
        """Generates a cassette name for the current test.

        The default format is "%(classname)s.%(testMethodName)s"

        To change the default cassette format, override this method in a
        subclass.

        :returns: Cassette name for the current test.
        :rtype: str
        """
        cls = getattr(self, '__class__')
        test = self._testMethodName
        return '{0}.{1}'.format(cls.__name__, test)

    def setUp(self):
        """Betamax-ified setUp fixture.

        This will call the superclass' setUp method *first* and then it will
        create a new :class:`requests.Session` and wrap that in a Betamax
        object to record it. At the end of ``setUp``, it will start recording.
        """
        # Bail out early if the SESSION_CLASS isn't a subclass of
        # requests.Session
        self.assertTrue(issubclass(self.SESSION_CLASS, requests.Session))
        # Make sure if the user is multiply inheriting that all setUps are
        # called. (If that confuses you, see: https://youtu.be/EiOglTERPEo)
        super(BetamaxTestCase, self).setUp()

        cassette_name = self.generate_cassette_name()

        self.session = self.SESSION_CLASS()
        self.recorder = recorder.Betamax(
            session=self.session,
            cassette_library_dir=self.CASSETTE_LIBRARY_DIR)

        self.recorder.use_cassette(cassette_name)
        self.recorder.start()

    def tearDown(self):
        """Betamax-ified tearDown fixture.

        This will call the superclass' tearDown method *first* and then it
        will stop recording interactions.
        """
        super(BetamaxTestCase, self).tearDown()
        self.recorder.stop()
