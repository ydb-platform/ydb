from typing import Optional, List
from thinc.types import Floats2d
from thinc.api import Model, zero_init, use_ops

from spacy.tokens import Doc
from spacy.compat import Literal
from spacy.errors import Errors
from spacy.util import registry


def TransitionBasedParser_v1(
    tok2vec: Model[List[Doc], List[Floats2d]],
    state_type: Literal["parser", "ner"],
    extra_state_tokens: bool,
    hidden_width: int,
    maxout_pieces: int,
    use_upper: bool = True,
    nO: Optional[int] = None,
) -> Model:

    chain = registry.get("layers", "chain.v1")
    list2array = registry.get("layers", "list2array.v1")
    Linear = registry.get("layers", "Linear.v1")
    TransitionModel = registry.get("layers", "spacy.TransitionModel.v1")
    PrecomputableAffine = registry.get("layers", "spacy.PrecomputableAffine.v1")

    if state_type == "parser":
        nr_feature_tokens = 13 if extra_state_tokens else 8
    elif state_type == "ner":
        nr_feature_tokens = 6 if extra_state_tokens else 3
    else:
        raise ValueError(Errors.E917.format(value=state_type))
    t2v_width = tok2vec.get_dim("nO") if tok2vec.has_dim("nO") else None
    tok2vec = chain(tok2vec, list2array(), Linear(hidden_width, t2v_width))
    tok2vec.set_dim("nO", hidden_width)
    lower = PrecomputableAffine(
        nO=hidden_width if use_upper else nO,
        nF=nr_feature_tokens,
        nI=tok2vec.get_dim("nO"),
        nP=maxout_pieces,
    )
    if use_upper:
        with use_ops("numpy"):
            # Initialize weights at zero, as it's a classification layer.
            upper = Linear(nO=nO, init_W=zero_init)
    else:
        upper = None
    return TransitionModel(tok2vec, lower, upper, resize_output=resize_output_v1)


def resize_output_v1(model, new_nO):
    Linear = registry.get("layers", "Linear.v1")

    lower = model.get_ref("lower")
    upper = model.get_ref("upper")
    if not model.attrs["has_upper"]:
        if lower.has_dim("nO") is None:
            lower.set_dim("nO", new_nO)
        return
    elif upper.has_dim("nO") is None:
        upper.set_dim("nO", new_nO)
        return
    elif new_nO == upper.get_dim("nO"):
        return
    smaller = upper
    nI = None
    if smaller.has_dim("nI"):
        nI = smaller.get_dim("nI")
    with use_ops("numpy"):
        larger = Linear(nO=new_nO, nI=nI)
        larger.init = smaller.init
    # it could be that the model is not initialized yet, then skip this bit
    if nI:
        larger_W = larger.ops.alloc2f(new_nO, nI)
        larger_b = larger.ops.alloc1f(new_nO)
        smaller_W = smaller.get_param("W")
        smaller_b = smaller.get_param("b")
        # Weights are stored in (nr_out, nr_in) format, so we're basically
        # just adding rows here.
        if smaller.has_dim("nO"):
            larger_W[: smaller.get_dim("nO")] = smaller_W
            larger_b[: smaller.get_dim("nO")] = smaller_b
            for i in range(smaller.get_dim("nO"), new_nO):
                model.attrs["unseen_classes"].add(i)

        larger.set_param("W", larger_W)
        larger.set_param("b", larger_b)
    model._layers[-1] = larger
    model.set_ref("upper", larger)
    return model
