# This is a very weak test, but testing Cython code can be hard. So, at least check
# we can create the object...

from cymem.cymem import Pool, Address


def test_pool():
    mem = Pool()
    assert mem.size == 0

def test_address():
    address = Address(1, 2)
