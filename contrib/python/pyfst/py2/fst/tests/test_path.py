import fst
from nose.tools import eq_

def test_paths():
    t = fst.Acceptor()
    t.add_arc(0, 1, 'a')
    t.add_arc(1, 2, 'b')
    t.add_arc(0, 2, 'c')
    t.add_arc(2, 3, 'd')
    t.add_arc(3, 4, 'e')
    t[2].final = True
    t[4].final = True

    words = set(''.join(t.isyms.find(arc.ilabel) for arc in path) for path in t.paths())
    eq_(words, set(('ab', 'c', 'abde', 'cde')))

if __name__ == '__main__':
    test_paths()
