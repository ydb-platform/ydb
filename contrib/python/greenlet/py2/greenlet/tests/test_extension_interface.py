from __future__ import print_function
from __future__ import absolute_import

import sys
import unittest

import greenlet
from . import _test_extension


class CAPITests(unittest.TestCase):
    def test_switch(self):
        self.assertEqual(
            50, _test_extension.test_switch(greenlet.greenlet(lambda: 50)))

    def test_switch_kwargs(self):
        def foo(x, y):
            return x * y
        g = greenlet.greenlet(foo)
        self.assertEqual(6, _test_extension.test_switch_kwargs(g, x=3, y=2))

    def test_setparent(self):
        def foo():
            def bar():
                greenlet.getcurrent().parent.switch()

                # This final switch should go back to the main greenlet, since
                # the test_setparent() function in the C extension should have
                # reparented this greenlet.
                greenlet.getcurrent().parent.switch()
                raise AssertionError("Should never have reached this code")
            child = greenlet.greenlet(bar)
            child.switch()
            greenlet.getcurrent().parent.switch(child)
            greenlet.getcurrent().parent.throw(
                AssertionError("Should never reach this code"))
        foo_child = greenlet.greenlet(foo).switch()
        self.assertEqual(None, _test_extension.test_setparent(foo_child))

    def test_getcurrent(self):
        _test_extension.test_getcurrent()

    def test_new_greenlet(self):
        self.assertEqual(-15, _test_extension.test_new_greenlet(lambda: -15))

    def test_raise_greenlet_dead(self):
        self.assertRaises(
            greenlet.GreenletExit, _test_extension.test_raise_dead_greenlet)

    def test_raise_greenlet_error(self):
        self.assertRaises(
            greenlet.error, _test_extension.test_raise_greenlet_error)

    def test_throw(self):
        seen = []

        def foo():
            try:
                greenlet.getcurrent().parent.switch()
            except ValueError:
                seen.append(sys.exc_info()[1])
            except greenlet.GreenletExit:
                raise AssertionError
        g = greenlet.greenlet(foo)
        g.switch()
        _test_extension.test_throw(g)
        self.assertEqual(len(seen), 1)
        self.assertTrue(
            isinstance(seen[0], ValueError),
            "ValueError was not raised in foo()")
        self.assertEqual(
            str(seen[0]),
            'take that sucka!',
            "message doesn't match")

if __name__ == '__main__':
    unittest.main()
