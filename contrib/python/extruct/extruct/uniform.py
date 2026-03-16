# mypy: disallow_untyped_defs=False
import copy
from typing import Any
from urllib.parse import urljoin, urlparse

from extruct.dublincore import get_lower_attrib


def _uopengraph(extracted, with_og_array=False):
    out = []
    for obj in extracted:
        # In order of appearance in the page
        properties = list(obj["properties"])
        flattened: dict[Any, Any] = {}

        for k, v in properties:
            if k not in flattened.keys():
                flattened[k] = v
            elif v and v.strip():
                # If og_array isn't required add first non empty value
                if not with_og_array:
                    if not flattened[k] or not flattened[k].strip():
                        flattened[k] = v
                else:
                    if isinstance(flattened[k], list):
                        flattened[k].append(v)
                    elif flattened[k] and flattened[k].strip():
                        flattened[k] = [flattened[k], v]
                    else:
                        flattened[k] = v

        t = flattened.pop("og:type", None)
        if t:
            flattened["@type"] = t
        flattened["@context"] = obj["namespace"]
        out.append(flattened)
    return out


def _umicrodata_microformat(extracted, schema_context):
    res = []
    if isinstance(extracted, list):
        for obj in extracted:
            res.append(flatten_dict(obj, schema_context, True))
    elif isinstance(extracted, dict):
        res.append(flatten_dict(extracted, schema_context, False))
    return res


def _udublincore(extracted):
    out = []
    extracted_cpy = copy.deepcopy(extracted)
    for obj in extracted_cpy:
        context = obj.pop("namespaces", None)
        obj["@context"] = context
        elements = obj["elements"]
        for element in elements:
            for key, value in element.items():
                if get_lower_attrib(value) == "type":
                    obj["@type"] = element["content"]
                    obj["elements"].remove(element)
                    break
        out.append(obj)
    return out


def _flatten(element, schema_context):
    if isinstance(element, dict):
        element = flatten_dict(element, schema_context, False)
    elif isinstance(element, list):
        element = [
            flatten_dict(o, schema_context, False) if isinstance(o, dict) else o
            for o in element
        ]
    return element


def flatten_dict(d, schema_context, add_context):
    out = dict(d)
    typ = out.pop("type", None)
    if not typ:
        return d

    if isinstance(typ, list):
        out["@type"] = typ
        context = schema_context
    else:
        context, typ = infer_context(typ, schema_context)
        out["@type"] = typ

    if add_context:
        out["@context"] = context

    props = out.pop("properties", {})
    for field, value in props.items():
        value = _flatten(value, schema_context)
        out[field] = value

    children = out.pop("children", [])
    if children:
        out["children"] = []
    for child in children:
        child = _flatten(child, schema_context)
        out["children"].append(child)
    return out


def infer_context(typ, context="http://schema.org"):
    parsed_context = urlparse(typ)
    if parsed_context.netloc:
        base = "".join([parsed_context.scheme, "://", parsed_context.netloc])
        if parsed_context.path and parsed_context.fragment:
            context = urljoin(base, parsed_context.path)
            typ = parsed_context.fragment.strip("/")
        elif parsed_context.path:
            context = base
            typ = parsed_context.path.strip("/")
    return context, typ
