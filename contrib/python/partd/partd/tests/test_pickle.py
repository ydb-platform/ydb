from partd.pickle import Pickle


import os
import shutil

def test_pickle():
    with Pickle() as p:
        p.append({'x': ['Hello', 'World!'], 'y': [1, 2, 3]})
        p.append({'x': ['Alice', 'Bob!'], 'y': [4, 5, 6]})
        assert os.path.exists(p.partd.filename('x'))
        assert os.path.exists(p.partd.filename('y'))

        result = p.get(['y', 'x'])
        assert result == [[1, 2, 3, 4, 5, 6],
                          ['Hello', 'World!', 'Alice', 'Bob!']]

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)

    assert not os.path.exists(p.partd.path)


def test_ensure():
    with Pickle() as p:
        p.iset('x', [1, 2, 3])
        p.iset('x', [1, 2, 3])

        assert p.get('x') == [1, 2, 3]
