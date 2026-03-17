# Copyright (c) 2015 Marc Brinkmann

# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.


class Visitor(object):
    """Base class for visitors."""

    def visit(self, node):
        """Visit a node.

        Calls ``visit_CLASSNAME`` on itself passing ``node``, where
        ``CLASSNAME`` is the node's class. If the visitor does not implement an
        appropriate visitation method, will go up the
        `MRO <https://www.python.org/download/releases/2.3/mro/>`_ until a
        match is found.

        If the search exhausts all classes of node, raises a
        :class:`~exceptions.NotImplementedError`.

        :param node: The node to visit.
        :return: The return value of the called visitation function.
        """
        if isinstance(node, type):
            mro = node.mro()
        else:
            mro = type(node).mro()
        for cls in mro:
            meth = getattr(self, 'visit_' + cls.__name__, None)
            if meth is None:
                continue
            return meth(node)

        raise NotImplementedError('No visitation method visit_{}'
                                  .format(node.__class__.__name__))
