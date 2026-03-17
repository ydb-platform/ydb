from thinc.layers.gelu import Gelu
from thinc.layers.parametricattention_v2 import (
    KEY_TRANSFORM_REF,
    ParametricAttention_v2,
)


def test_key_transform_used():
    attn = ParametricAttention_v2(key_transform=Gelu())
    assert attn.get_ref(KEY_TRANSFORM_REF).name == "gelu"
