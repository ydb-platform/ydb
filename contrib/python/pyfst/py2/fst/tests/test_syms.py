import fst
from nose.tools import eq_, ok_, assert_raises

def test_syms():
    syms = fst.SymbolTable()
    eq_(len(syms), 1) # __len__
    ok_(fst.EPSILON in syms) # __contains__
    eq_(syms[fst.EPSILON], fst.EPSILON_ID) # __getitem__
    eq_(syms.find(fst.EPSILON_ID), fst.EPSILON) # find(int)
    eq_(syms.find(fst.EPSILON), fst.EPSILON_ID) # find(str)
    eq_(syms, syms.copy()) # __richcmp__
    syms['a'] = 2 # __setitem__
    eq_(syms.find('a'), 2)
    eq_(syms.find(2), 'a')
    eq_(syms['a'], 2)
    assert_raises(KeyError, syms.find, 'x')
    assert_raises(KeyError, syms.find, 1)
    ok_('x' not in syms)
    eq_(list(syms.items()), [(fst.EPSILON, fst.EPSILON_ID), ('a', 2)])

def test_merge():
    # Good merge
    syms1 = fst.SymbolTable()
    eq_(syms1['a'], 1)
    eq_(syms1['b'], 2)

    syms2 = fst.SymbolTable()
    syms2['a'] = 1
    syms2['c'] = 3

    syms1.merge(syms2)
    eq_(list(syms1.items()), [(fst.EPSILON, fst.EPSILON_ID),
        ('a', 1), ('b', 2), ('c', 3)])

    # Bad merge (value conflict: a -> 1 vs a -> 2)
    syms3 = fst.SymbolTable()
    syms3['a'] = 2
    assert_raises(ValueError, syms2.merge, syms3)

    # Bad merge (symbol conflict: a -> 1 vs b -> 1)
    syms4 = fst.SymbolTable()
    syms4['b'] = 1
    assert_raises(ValueError, syms2.merge, syms4)

if __name__ == '__main__':
    test_syms()
    test_merge()
