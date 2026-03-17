from partd.compressed import ZLib


import shutil
import os
import pickle


def test_partd():
    with ZLib() as p:
        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})
        assert os.path.exists(p.partd.filename('x'))
        assert os.path.exists(p.partd.filename('y'))

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert p.get('z') == b''

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)

    assert not os.path.exists(p.partd.path)


def test_pickle():
    with ZLib() as p:
        p.append({'x': b'123'})
        q = pickle.loads(pickle.dumps(p))
        assert q.get('x') == b'123'
