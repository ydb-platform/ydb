import pytest
try:
    import mock
except ImportError:
    import unittest.mock as mock

from dominate.tags import *


def test___get_thread_context(monkeypatch):
    from dominate import dom_tag as sut
    greenlet = mock.Mock()
    greenlet.getcurrent.return_value = 100
    monkeypatch.setattr(sut, 'greenlet', greenlet)

    threading = mock.Mock()
    threading.current_thread.return_value = 200
    monkeypatch.setattr(sut, 'threading', threading)

    assert sut._get_thread_context() is not None

def test_add_raw_string():
    container = div()
    container.add_raw_string('foo')
    assert container.children == ['foo']

def test_clear():
    container = div()
    child = div()
    container.add(child)

    assert container.children == [child]
    assert child.parent == container

    container.clear()

    assert container.children == []
    assert child.parent is None

def test_set_attribute():
    container = div()
    container.add_raw_string('foo')
    container.set_attribute(0, 'bar')

    assert container.children == ['bar']

def test_set_attribute_error():
    container = div()
    with pytest.raises(TypeError, match=(
            'Only integer and string types are valid for assigning '
            'child tags and attributes, respectively.'
        )):
        container.set_attribute(1.0, 'foo')

def test___get_item___child_index_error():
    d = div()
    with pytest.raises(IndexError, match='Child with index "10" does not exist.'):
        d[10]

def test___contains__():
    container = div()
    container.add(div())
    assert 'div' in container

def test_nested_context():
    def sub(*args):
        with div('B') as B:
            B.add(*args)

    with div('A') as A:
        sub(div('C'))

    assert str(A) == \
'''<div>A
  <div>B
    <div>C</div>
  </div>
</div>'''
