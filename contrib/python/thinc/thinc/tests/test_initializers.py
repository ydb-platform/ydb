import numpy
import pytest

from thinc import registry
from thinc.api import (
    NumpyOps,
    glorot_uniform_init,
    normal_init,
    uniform_init,
    zero_init,
)


@pytest.mark.parametrize(
    "init_func", [glorot_uniform_init, zero_init, uniform_init, normal_init]
)
def test_initializer_func_setup(init_func):
    ops = NumpyOps()
    data = numpy.ndarray([1, 2, 3, 4], dtype="f")
    result = init_func(ops, data.shape)
    assert not numpy.array_equal(data, result)


@pytest.mark.parametrize(
    "name,kwargs",
    [
        ("glorot_uniform_init.v1", {}),
        ("zero_init.v1", {}),
        ("uniform_init.v1", {"lo": -0.5, "hi": 0.5}),
        ("normal_init.v1", {"mean": 0.1}),
    ],
)
def test_initializer_from_config(name, kwargs):
    """Test that initializers are loaded and configured correctly from registry
    (as partials)."""
    cfg = {"test": {"@initializers": name, **kwargs}}
    func = registry.resolve(cfg)["test"]
    func(NumpyOps(), (1, 2, 3, 4))
