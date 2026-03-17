import greenlet
import unittest


class Test(unittest.TestCase):

    def test_stack_saved(self):
        main = greenlet.getcurrent()
        self.assertEqual(main._stack_saved, 0)

        def func():
            main.switch(main._stack_saved)

        g = greenlet.greenlet(func)
        x = g.switch()
        assert x > 0, x
        assert g._stack_saved > 0, g._stack_saved
        g.switch()
        assert g._stack_saved == 0, g._stack_saved
