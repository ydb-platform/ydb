import unittest

import parsley


def wrapperFactory(addition):
    def wrapper(wrapped):
        return addition, wrapped
    return wrapper

def nullFactory(*args):
    return args


class StackTestCase(unittest.TestCase):
    def test_onlyBase(self):
        "stack can be called with no wrappers."
        fac = parsley.stack(nullFactory)
        self.assertEqual(fac('a'), ('a',))

    def test_oneWrapper(self):
        "stack can be called with one wrapper."
        fac = parsley.stack(wrapperFactory(0), nullFactory)
        self.assertEqual(fac('a'), (0, ('a',)))

    def test_tenWrappers(self):
        "stack can be called with ten wrappers."
        args = []
        result = 'a',
        for x in range(10):
            args.append(wrapperFactory(x))
            result = 9 - x, result
        args.append(nullFactory)
        fac = parsley.stack(*args)
        self.assertEqual(fac('a'), result)

    def test_failsWithNoBaseSender(self):
        "stack does require at least the base factory."
        self.assertRaises(TypeError, parsley.stack)

    def test_senderFactoriesTakeOneArgument(self):
        "The callable returned by stack takes exactly one argument."
        fac = parsley.stack(nullFactory)
        self.assertRaises(TypeError, fac)
        self.assertRaises(TypeError, fac, 'a', 'b')
