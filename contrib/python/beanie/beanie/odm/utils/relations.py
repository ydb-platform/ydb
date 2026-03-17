from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Dict
from typing import Mapping as MappingType

from beanie.odm.fields import (
    ExpressionField,
)

# from pydantic.fields import ModelField
# from pydantic.typing import get_origin

if TYPE_CHECKING:
    from beanie import Document


def convert_ids(
    query: MappingType[str, Any], doc: "Document", fetch_links: bool
) -> Dict[str, Any]:
    # TODO add all the cases
    new_query = {}
    for k, v in query.items():
        k_splitted = k.split(".")
        if (
            isinstance(k, ExpressionField)
            and doc.get_link_fields() is not None
            and len(k_splitted) == 2
            and k_splitted[0] in doc.get_link_fields().keys()  # type: ignore
            and k_splitted[1] == "id"
        ):
            if fetch_links:
                new_k = f"{k_splitted[0]}._id"
            else:
                new_k = f"{k_splitted[0]}.$id"
        else:
            new_k = k
        new_v: Any
        if isinstance(v, Mapping):
            new_v = convert_ids(v, doc, fetch_links)
        elif isinstance(v, list):
            new_v = [
                convert_ids(ele, doc, fetch_links)
                if isinstance(ele, Mapping)
                else ele
                for ele in v
            ]
        else:
            new_v = v

        new_query[new_k] = new_v
    return new_query
