"""Helper objects needed by multiple CLI commands.

"""

from functools import partial
import json
import math
import warnings

from fiona.model import Geometry, to_dict
from fiona._vendor.munch import munchify


warnings.simplefilter("default")


def obj_gen(lines, object_hook=None):
    """Return a generator of JSON objects loaded from ``lines``."""
    first_line = next(lines)
    if first_line.startswith("\x1e"):

        def gen():
            buffer = first_line.strip("\x1e")
            for line in lines:
                if line.startswith("\x1e"):
                    if buffer:
                        yield json.loads(buffer, object_hook=object_hook)
                    buffer = line.strip("\x1e")
                else:
                    buffer += line
            else:
                yield json.loads(buffer, object_hook=object_hook)

    else:

        def gen():
            yield json.loads(first_line, object_hook=object_hook)
            for line in lines:
                yield json.loads(line, object_hook=object_hook)

    return gen()


def nullable(val, cast):
    if val is None:
        return None
    else:
        return cast(val)


def eval_feature_expression(feature, expression):
    safe_dict = {"f": munchify(to_dict(feature))}
    safe_dict.update(
        {
            "sum": sum,
            "pow": pow,
            "min": min,
            "max": max,
            "math": math,
            "bool": bool,
            "int": partial(nullable, int),
            "str": partial(nullable, str),
            "float": partial(nullable, float),
            "len": partial(nullable, len),
        }
    )
    try:
        from shapely.geometry import shape

        safe_dict["shape"] = shape
    except ImportError:
        pass
    return eval(expression, {"__builtins__": None}, safe_dict)


def make_ld_context(context_items):
    """Returns a JSON-LD Context object.

    See https://json-ld.org/spec/latest/json-ld/."""
    ctx = {
        "@context": {
            "geojson": "http://ld.geojson.org/vocab#",
            "Feature": "geojson:Feature",
            "FeatureCollection": "geojson:FeatureCollection",
            "GeometryCollection": "geojson:GeometryCollection",
            "LineString": "geojson:LineString",
            "MultiLineString": "geojson:MultiLineString",
            "MultiPoint": "geojson:MultiPoint",
            "MultiPolygon": "geojson:MultiPolygon",
            "Point": "geojson:Point",
            "Polygon": "geojson:Polygon",
            "bbox": {"@container": "@list", "@id": "geojson:bbox"},
            "coordinates": "geojson:coordinates",
            "datetime": "http://www.w3.org/2006/time#inXSDDateTime",
            "description": "http://purl.org/dc/terms/description",
            "features": {"@container": "@set", "@id": "geojson:features"},
            "geometry": "geojson:geometry",
            "id": "@id",
            "properties": "geojson:properties",
            "start": "http://www.w3.org/2006/time#hasBeginning",
            "stop": "http://www.w3.org/2006/time#hasEnding",
            "title": "http://purl.org/dc/terms/title",
            "type": "@type",
            "when": "geojson:when",
        }
    }
    for item in context_items or []:
        t, uri = item.split("=")
        ctx[t.strip()] = uri.strip()
    return ctx


def id_record(rec):
    """Converts a record's id to a blank node id and returns the record."""
    rec["id"] = f"_:f{rec['id']}"
    return rec


def recursive_round(obj, precision):
    """Recursively round coordinates."""
    if precision < 0:
        return obj
    if getattr(obj, "geometries", None):
        return Geometry(
            geometries=[recursive_round(part, precision) for part in obj.geometries]
        )
    elif getattr(obj, "coordinates", None):
        return Geometry(
            coordinates=[recursive_round(part, precision) for part in obj.coordinates]
        )
    if isinstance(obj, (int, float)):
        return round(obj, precision)
    else:
        return [recursive_round(part, precision) for part in obj]
