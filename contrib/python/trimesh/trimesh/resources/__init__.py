import gzip
import importlib.resources
import json
import os
from io import BytesIO

from ..typed import Dict

# find the current absolute path to this directory
_pwd = importlib.resources.files(__package__)
# once resources are loaded cache them
_cache = {}


def get_schema(name: str) -> Dict:
    """
    Load a schema and evaluate the referenced files.

    Parameters
    ------------
    name : str
      Filename of schema.

    Returns
    ----------
    schema
      Loaded and resolved schema.
    """
    from ..resolvers import FilePathResolver
    from ..schemas import resolve

    # get a resolver for our base path
    with importlib.resources.as_file(_pwd / "schema" / name) as fn:
        resolver = FilePathResolver(fn)
    # recursively load `$ref` keys
    return resolve(json.loads(resolver.get(name).decode("utf-8")), resolver=resolver)


def get_json(name: str) -> Dict:
    """
    Get a resource from the `trimesh/resources` folder as a decoded string.

    Parameters
    -------------
    name : str
      File path relative to `trimesh/resources/{name}`

    Returns
    -------------
    resource
      File data decoded from JSON.
    """
    raw = get_bytes(name)
    if name.endswith(".gzip"):
        raw = gzip.decompress(raw)
    else:
        raw = raw.decode("utf-8")
    return json.loads(raw)


def get_string(name: str) -> str:
    """
    Get a resource from the `trimesh/resources` folder as a decoded string.

    Parameters
    -------------
    name
      File path relative to `trimesh/resources`

    Returns
    -------------
    resource
      File data as a string.
    """
    return get_bytes(name).decode("utf-8")


def get_bytes(name: str) -> bytes:
    """
    Get a resource from the `trimesh/resources` folder as binary data.

    Parameters
    -------------
    name
      File path relative to `trimesh/resources`

    Returns
    -------------
    resource
      File data as raw bytes.
    """
    cached = _cache.get(name, None)
    if cached is not None:
        return cached

    # get the resource using relative names
    # all templates are using POSIX relative paths
    # so fix them to be platform-specific
    with _pwd.joinpath(*name.split("/")).open("rb") as f:
        resource = f.read()

    _cache[name] = resource
    return resource


def get_stream(name: str) -> BytesIO:
    """
    Get a resource from the `trimesh/resources` folder as a binary stream.

    Parameters
    -------------
    name : str
      File path relative to `trimesh/resources`

    Returns
    -------------
    resource
      File data as a binary stream.
    """

    return BytesIO(get_bytes(name))
