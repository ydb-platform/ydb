import pickle
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from multidict import MultiDict, MultiDictProxy

if TYPE_CHECKING:
    from conftest import MultidictImplementation

import yatest.common as yc
here = Path(yc.source_path(__file__)).resolve().parent


def test_pickle(
    any_multidict_class: type[MultiDict[int]], pickle_protocol: int
) -> None:
    d = any_multidict_class([("a", 1), ("a", 2)])
    pbytes = pickle.dumps(d, pickle_protocol)
    obj = pickle.loads(pbytes)
    assert d == obj
    assert isinstance(obj, any_multidict_class)


def test_pickle_proxy(
    any_multidict_class: type[MultiDict[int]],
    any_multidict_proxy_class: type[MultiDictProxy[int]],
) -> None:
    d = any_multidict_class([("a", 1), ("a", 2)])
    proxy = any_multidict_proxy_class(d)
    with pytest.raises(TypeError):
        pickle.dumps(proxy)


def test_load_from_file(
    any_multidict_class: type[MultiDict[int]],
    multidict_implementation: "MultidictImplementation",
    pickle_protocol: int,
) -> None:
    multidict_class_name = any_multidict_class.__name__
    pickle_file_basename = "-".join(
        (
            multidict_class_name.lower(),
            multidict_implementation.tag,
        )
    )
    d = any_multidict_class([("a", 1), ("a", 2)])
    fname = f"{pickle_file_basename}.pickle.{pickle_protocol}"
    p = here / fname
    with p.open("rb") as f:
        obj = pickle.load(f)
    assert d == obj
    assert isinstance(obj, any_multidict_class)
