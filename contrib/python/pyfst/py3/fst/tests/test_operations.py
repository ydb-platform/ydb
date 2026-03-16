import fst
from nose.tools import eq_

def test_shortest_distance():
    t = fst.Acceptor()
    t.add_arc(0, 1, 'a', 3)
    t.add_arc(1, 1, 'b', 2)
    t.add_arc(1, 3, 'c', 4)
    t.add_arc(0, 2, 'd', 5)
    t.add_arc(2, 3, 'f', 4)
    t[3].final = 3

    eq_([float(v) for v in t.shortest_distance()], [0, 3, 5, 7])
    eq_([float(v) for v in t.shortest_distance(True)], [10, 7, 7, 3])

def test_replace():
    syms = fst.SymbolTable()

    a1 = fst.Acceptor(syms)
    a1.add_arc(0, 1, 'dial')
    a1.add_arc(1, 2, 'google')
    a1.add_arc(1, 2, '$name')
    a1.add_arc(2, 3, 'please')
    a1[3].final = True

    a2 = fst.Acceptor(syms)
    a2.add_arc(0, 1, 'michael')
    a2.add_arc(1, 2, 'riley')
    a2.add_arc(0, 1, '$firstname')
    a2.add_arc(1, 2, '$lastname')
    a2[2].final = True

    a3 = fst.Acceptor(syms)
    a3.add_arc(0, 1, 'johan')
    a3[1].final = True

    a4 = fst.Acceptor(syms)
    a4.add_arc(0, 1, 'schalkwyk')
    a4[1].final = True

    result = a1.replace({'$name': a2, '$firstname': a3, '$lastname': a4}, epsilon=True)
    result.remove_epsilon()

    expected = fst.Acceptor(syms)
    expected.add_arc(0, 1, 'dial')
    expected.add_arc(1, 2, 'google')
    expected.add_arc(1, 3, fst.EPSILON)
    expected.add_arc(3, 5, 'michael')
    expected.add_arc(3, 6, fst.EPSILON)
    expected.add_arc(6, 9, 'johan')
    expected.add_arc(9, 5, fst.EPSILON)
    expected.add_arc(5, 7, 'riley')
    expected.add_arc(5, 8, fst.EPSILON)
    expected.add_arc(8, 10, 'schalkwyk')
    expected.add_arc(10, 7, fst.EPSILON)
    expected.add_arc(7, 2, fst.EPSILON)
    expected.add_arc(2, 4, 'please')
    expected[4].final = True
    expected.remove_epsilon()

    eq_(result, expected)

# TODO generate several paths and check number of paths generated
# TODO check distributions?
def test_randgen():
    t = fst.Acceptor()
    t.add_arc(0, 1, 'a', 0.5)
    t.add_arc(1, 2, 'b', 0.5)
    t.add_arc(0, 2, 'ab', 1.0)
    t.add_arc(2, 3, 'c')
    t.add_arc(3, 4, 'd')
    t.add_arc(2, 4, 'cd')
    t[4].final = True
    r = t.uniform_generate()
    # check that r \in t
    eq_(r & t.remove_weights(), r)
    r = t.logprob_generate()
    # check that r \in t
    eq_(r & t.remove_weights(), r)

def test_closure():
    t = fst.linear_chain('ab')
    result = t.closure_plus()
    eq_(len(result), len(t))
    result.remove_epsilon()
    expected = t + t.closure()
    expected.remove_epsilon()
    eq_(result, expected)

if __name__ == '__main__':
    test_shortest_distance()
    test_replace()
