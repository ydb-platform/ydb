

class DatabaseException(Exception):
    def __init__(self, orig):
        self.orig = orig
        super(DatabaseException, self).__init__(orig)

    def __str__(self):
        return 'Orig exception: {}'.format(self.orig)
