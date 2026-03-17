

from .fact import fact
from .attribute import AttributeScheme
from .normalizer import (
    InflectedNormalizer,
    NormalizedNormalizer,
    ConstNormalizer,
    FunctionNormalizer
)
from .interpretator import (
    Interpretator,
    prepare_token_interpretator,
    prepare_rule_interpretator
)


attribute = AttributeScheme

normalized = NormalizedNormalizer
inflected = InflectedNormalizer
const = ConstNormalizer
custom = FunctionNormalizer
