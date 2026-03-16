import murmurhash.mrmr


def test_hashes():
    assert murmurhash.mrmr.hash("hello world") == 1586663183
    assert murmurhash.mrmr.hash("anxiety") == -1859125401
