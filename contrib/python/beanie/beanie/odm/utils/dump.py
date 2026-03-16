from typing import TYPE_CHECKING, Optional, Set

from beanie.odm.utils.encoder import Encoder

if TYPE_CHECKING:
    from beanie.odm.documents import Document


def get_dict(
    document: "Document",
    to_db: bool = False,
    exclude: Optional[Set[str]] = None,
    keep_nulls: bool = True,
):
    if exclude is None:
        exclude = set()
    if document.id is None:
        exclude.add("_id")
    if not document.get_settings().use_revision:
        exclude.add("revision_id")
    encoder = Encoder(exclude=exclude, to_db=to_db, keep_nulls=keep_nulls)
    return encoder.encode(document)


def get_nulls(
    document: "Document",
    exclude: Optional[Set[str]] = None,
):
    dictionary = get_dict(document, exclude=exclude, keep_nulls=True)
    return filter_none(dictionary)


def get_top_level_nones(
    document: "Document",
    exclude: Optional[Set[str]] = None,
):
    dictionary = get_dict(document, exclude=exclude, keep_nulls=True)
    return {k: v for k, v in dictionary.items() if v is None}


def filter_none(d):
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            filtered = filter_none(v)
            if filtered:
                result[k] = filtered
        elif v is None:
            result[k] = v
    return result
