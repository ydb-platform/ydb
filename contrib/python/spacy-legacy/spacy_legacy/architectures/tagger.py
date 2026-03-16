from typing import Optional, List
from thinc.api import Model
from thinc.types import Floats2d

from spacy.util import registry
from spacy.tokens import Doc


def Tagger_v1(
    tok2vec: Model[List[Doc], List[Floats2d]], nO: Optional[int] = None
) -> Model[List[Doc], List[Floats2d]]:
    """Build a tagger model, using a provided token-to-vector component. The tagger
    model simply adds a linear layer with softmax activation to predict scores
    given the token vectors.

    tok2vec (Model[List[Doc], List[Floats2d]]): The token-to-vector subnetwork.
    nO (int or None): The number of tags to output. Inferred from the data if None.
    """
    # TODO: glorot_uniform_init seems to work a bit better than zero_init here?!
    chain = registry.get("layers", "chain.v1")
    softmax = registry.get("layers", "Softmax.v1")
    with_array = registry.get("layers", "with_array.v1")
    zero_init = registry.get("initializers", "zero_init.v1")

    t2v_width = tok2vec.get_dim("nO") if tok2vec.has_dim("nO") else None
    output_layer = softmax(nO, t2v_width, init_W=zero_init())
    model = chain(tok2vec, with_array(output_layer))  # type: ignore
    model.set_ref("tok2vec", tok2vec)
    model.set_ref("softmax", output_layer)
    model.set_ref("output_layer", output_layer)
    return model
