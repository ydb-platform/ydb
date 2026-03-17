import unittest

class Clock(object):
    def __init__(self):
        self.reset()

    def __call__(self):
        return self.now

    def reset(self):
        self.now = 0

    def increment(self, num=1):
        self.now += num

clock = Clock()

__all__ = [
    'unittest',
    'clock'
]
