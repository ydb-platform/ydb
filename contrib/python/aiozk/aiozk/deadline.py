import time


class Deadline:
    """It is convenient to use this class instead of timeout variable if the
    timeout variable is accessed multiple times inside a function.
    """

    def __init__(self, timeout):
        if timeout is None:
            self.deadline = None
        else:
            self.deadline = timeout + time.time()

    @property
    def timeout(self):
        if self.deadline is None:
            return None
        else:
            return self.deadline - time.time()

    @property
    def has_passed(self):
        if self.deadline is None:
            return False
        else:
            return self.deadline - time.time() <= 0

    @property
    def is_indefinite(self):
        return True if self.deadline is None else False
