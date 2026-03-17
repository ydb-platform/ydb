from partd.dict import Dict

import shutil
import os


def test_partd():
    with Dict() as p:
        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert p.get('z') == b''

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)


def test_key_tuple():
    with Dict() as p:
        p.append({('a', 'b'): b'123'})
        assert p.get(('a', 'b')) == b'123'


def test_iset():
    with Dict() as p:
        p.iset('x', b'123')
        assert 'x' in p._iset_seen
        assert 'y' not in p._iset_seen
        p.iset('x', b'123')
        p.iset('x', b'123')
        assert p.get('x') == b'123'


def test_delete_non_existent_key():
    with Dict() as p:
        p.append({'x': b'123'})
        p.delete(['x', 'y'])
        assert p.get(['x', 'y']) == [b'', b'']
