from typing import List

from thinc.shims.shim import Shim

from ..util import make_tempdir


class MockShim(Shim):
    def __init__(self, data: List[int]):
        super().__init__(None, config=None, optimizer=None)
        self.data = data

    def to_bytes(self):
        return bytes(self.data)

    def from_bytes(self, data: bytes) -> "MockShim":
        return MockShim(data=list(data))


def test_shim_can_roundtrip_with_path():

    with make_tempdir() as path:
        shim_path = path / "cool_shim.data"
        shim = MockShim([1, 2, 3])
        shim.to_disk(shim_path)
        copy_shim = shim.from_disk(shim_path)
    assert copy_shim.to_bytes() == shim.to_bytes()


def test_shim_can_roundtrip_with_path_subclass(pathy_fixture):
    shim_path = pathy_fixture / "cool_shim.data"
    shim = MockShim([1, 2, 3])
    shim.to_disk(shim_path)
    copy_shim = shim.from_disk(shim_path)
    assert copy_shim.to_bytes() == shim.to_bytes()
