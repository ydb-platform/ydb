import numpy
import pytest

from thinc.api import (
    PyTorchWrapper_v2,
    TorchScriptWrapper_v1,
    pytorch_to_torchscript_wrapper,
)
from thinc.compat import has_torch, torch


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.parametrize("nN,nI,nO", [(2, 3, 4)])
def test_pytorch_script(nN, nI, nO):

    model = PyTorchWrapper_v2(torch.nn.Linear(nI, nO)).initialize()
    script_model = pytorch_to_torchscript_wrapper(model)

    X = numpy.random.randn(nN, nI).astype("f")
    Y = model.predict(X)
    Y_script = script_model.predict(X)
    numpy.testing.assert_allclose(Y, Y_script)

    serialized = script_model.to_bytes()
    script_model2 = TorchScriptWrapper_v1()
    script_model2.from_bytes(serialized)

    numpy.testing.assert_allclose(Y, script_model2.predict(X))
