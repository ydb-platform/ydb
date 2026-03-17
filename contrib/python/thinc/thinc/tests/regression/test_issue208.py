from thinc.api import Linear, chain


def test_issue208():
    """Test issue that was caused by trying to flatten nested chains."""
    layer1 = Linear(nO=9, nI=3)
    layer2 = Linear(nO=12, nI=9)
    layer3 = Linear(nO=5, nI=12)
    model = chain(layer1, chain(layer2, layer3)).initialize()
    assert model.get_dim("nO") == 5
