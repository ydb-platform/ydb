from email import parser, message
import sys


class MockHTTPResponse(object):
    def __init__(self, headers):
        from betamax.util import coerce_content

        h = ["%s: %s" % (k, v) for k in headers for v in headers.getlist(k)]
        h = map(coerce_content, h)
        h = '\r\n'.join(h)
        if sys.version_info < (2, 7):
            h = h.encode()
        p = parser.Parser(EmailMessage)
        # Thanks to Python 3, we have to use the slightly more awful API below
        # mimetools was deprecated so we have to use email.message.Message
        # which takes no arguments in its initializer.
        self.msg = p.parsestr(h)
        self.msg.set_payload(h)

        self._closed = False

    def isclosed(self):
        return self._closed

    def close(self):
        self._closed = True


class EmailMessage(message.Message):
    def getheaders(self, value, *args):
        return self.get_all(value, [])
