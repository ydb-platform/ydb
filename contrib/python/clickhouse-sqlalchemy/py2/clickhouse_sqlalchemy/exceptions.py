
from .util import compat


class DatabaseException(Exception):
    def __init__(self, orig):
        self.orig = orig
        super(DatabaseException, self).__init__(orig)

    def __str__(self):
        text = 'Orig exception: {}'.format(self.orig)

        if compat.PY3:
            return compat.text_type(text)

        else:
            return compat.text_type(text).encode('utf-8')
