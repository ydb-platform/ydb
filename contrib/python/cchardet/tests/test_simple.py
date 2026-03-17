"""
A very minimal non-upstream test.
"""

import cchardet


def test_simple_main():
    value = b'qwe\xe2\x8b\xaf'
    res = cchardet.detect(value)
    assert res['encoding'] == 'UTF-8'
    assert res['confidence'] > 0.01
