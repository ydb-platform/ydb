import fst
from nose.tools import eq_, ok_

def test_simple():
    t = fst.Transducer()
    for i, (ic, oc) in enumerate(zip('hello', 'olleh')):
        t.add_arc(i, i+1, ic, oc)
    t[i+1].final = True
    eq_(len(t), 6)
    ok_(t[5].final)

    a = fst.Acceptor()
    for i, c in enumerate('hello'):
        a.add_arc(i, i+1, c)
    a[i+1].final = True
    eq_(len(a), 6)
    ok_(a[5].final)

    l = fst.linear_chain('hello')
    eq_(a, l)

if __name__ == '__main__':
    test_simple()
