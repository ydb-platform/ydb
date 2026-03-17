import numpy

from thinc.backends._param_server import ParamServer


def test_param_server_init():
    array = numpy.zeros((5,), dtype="f")
    params = {("a", 1): array, ("b", 2): array}
    grads = {("a", 1): array, ("c", 3): array}
    ps = ParamServer(params, grads)
    assert ps.param_keys == (("a", 1), ("b", 2))
    assert ps.grad_keys == (("a", 1),)
