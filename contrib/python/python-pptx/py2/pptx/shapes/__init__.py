# encoding: utf-8

"""
Objects used across sub-package
"""


class Subshape(object):
    """
    Provides common services for drawing elements that occur below a shape
    but may occasionally require an ancestor object to provide a service,
    such as add or drop a relationship. Provides ``self._parent`` attribute
    to subclasses.
    """

    def __init__(self, parent):
        super(Subshape, self).__init__()
        self._parent = parent

    @property
    def part(self):
        """
        The package part containing this object
        """
        return self._parent.part
