# -*- coding: utf-8 -*-
from .base import BaseMatcher
from betamax.util import from_list


class DigestAuthMatcher(BaseMatcher):

    """This matcher is provided to help those who need to use Digest Auth.

    .. note::

        The code requests 2.0.1 uses to generate this header is different from
        the code that every requests version after it uses. Specifically, in
        2.0.1 one of the parameters is ``qop=auth`` and every other version is
        ``qop="auth"``. Given that there's also an unsupported type of ``qop``
        in requests, I've chosen not to ignore ore sanitize this. All
        cassettes recorded on 2.0.1 will need to be re-recorded for any
        requests version after it.

        This matcher also ignores the ``cnonce`` and ``response`` parameters.
        These parameters require the system time to be monkey-patched and
        that is out of the scope of betamax

    """

    name = 'digest-auth'

    def match(self, request, recorded_request):
        request_digest = self.digest_parts(request.headers)
        recorded_digest = self.digest_parts(recorded_request['headers'])
        return request_digest == recorded_digest

    def digest_parts(self, headers):
        auth = headers.get('Authorization') or headers.get('authorization')
        if not auth:
            return None
        auth = from_list(auth).strip('Digest ')
        # cnonce and response will be based on the system time, which I will
        # not monkey-patch.
        excludes = ('cnonce', 'response')
        return [p for p in auth.split(', ') if not p.startswith(excludes)]
