betamax
=======

Betamax is a VCR_ imitation for requests. This will make mocking out requests
much easier. It is tested on `Travis CI`_.

Put in a more humorous way: "Betamax records your HTTP interactions so the NSA
does not have to."

Example Use
-----------

.. code-block:: python

    from betamax import Betamax
    from requests import Session
    from unittest import TestCase

    with Betamax.configure() as config:
        config.cassette_library_dir = 'tests/fixtures/cassettes'


    class TestGitHubAPI(TestCase):
        def setUp(self):
            self.session = Session()
            self.headers.update(...)

        # Set the cassette in a line other than the context declaration
        def test_user(self):
            with Betamax(self.session) as vcr:
                vcr.use_cassette('user')
                resp = self.session.get('https://api.github.com/user',
                                        auth=('user', 'pass'))
                assert resp.json()['login'] is not None

        # Set the cassette in line with the context declaration
        def test_repo(self):
            with Betamax(self.session).use_cassette('repo'):
                resp = self.session.get(
                    'https://api.github.com/repos/sigmavirus24/github3.py'
                    )
                assert resp.json()['owner'] != {}

What does it even do?
---------------------

If you are unfamiliar with VCR_, you might need a better explanation of what
Betamax does.

Betamax intercepts every request you make and attempts to find a matching
request that has already been intercepted and recorded. Two things can then
happen:

1. If there is a matching request, it will return the response that is
   associated with it.
2. If there is **not** a matching request and it is allowed to record new
   responses, it will make the request, record the response and return the
   response.

Recorded requests and corresponding responses - also known as interactions -
are stored in files called cassettes. (An example cassette can be seen in
the `examples section of the documentation`_.) The directory you store your
cassettes in is called your library, or your `cassette library`_.

VCR Cassette Compatibility
--------------------------

Betamax can use any VCR-recorded cassette as of this point in time. The only
caveat is that python-requests returns a URL on each response. VCR does not
store that in a cassette now but we will. Any VCR-recorded cassette used to
playback a response will unfortunately not have a URL attribute on responses
that are returned. This is a minor annoyance but not something that can be
fixed.

.. _VCR: https://github.com/vcr/vcr
.. _Travis CI: https://travis-ci.org/sigmavirus24/betamax
.. _examples section of the documentation:
    http://betamax.readthedocs.org/en/latest/api.html#examples
.. _cassette library:
    http://betamax.readthedocs.org/en/latest/cassettes.html
