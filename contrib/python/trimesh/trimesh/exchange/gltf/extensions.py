"""
gltf_extensions.py
------------------

Extension registry for glTF import/export with scope-based handlers.
Each scope has a TypedDict defining the context passed to handlers.
"""

from typing import Any, Callable, OrderedDict, TypedDict

from ...constants import log
from ...typed import Dict, List, Literal, Optional

# Scopes define where in the glTF load/export process handlers run:
#   material            - after parsing material, can override PBR values
#   texture_source      - when resolving texture image index
#   primitive           - after loading primitive, can add face_attributes
#   primitive_preprocess - before accessor reads, can modify accessors in-place
#   primitive_export    - during mesh export, can compress/modify primitive data
Scope = Literal[
    "material", "texture_source", "primitive", "primitive_preprocess", "primitive_export"
]


# ----------------------------------------------------------------------
# TypedDict contexts for each scope
# ----------------------------------------------------------------------
#
# These TypedDicts define the MINIMUM fields passed to handlers for each scope.
# Additional fields may be added in future versions for new functionality.
#
# FOR FORWARD COMPATIBILITY: Handlers should access only the fields they need
# and ignore unknown fields. The context is passed as a plain dict at runtime,
# so handlers can safely use dict.get() for optional access or simply not
# reference fields they don't need.
#
# Example handler pattern:
#
#     def my_handler(context: MaterialContext) -> Optional[Dict]:
#         # Access only what you need - additional fields won't break this
#         data = context["data"]
#         images = context["images"]
#         return {"baseColorFactor": [1, 0, 0, 1]}
#
# ----------------------------------------------------------------------


class MaterialContext(TypedDict):
    """Context for material scope handlers."""

    data: Dict[str, Any]
    parse_textures: Callable[..., Dict[str, Any]]
    images: List


class TextureSourceContext(TypedDict):
    """Context for texture_source scope handlers."""

    data: Dict[str, Any]


class PrimitiveContext(TypedDict):
    """Context for primitive scope handlers (post-load)."""

    data: Dict[str, Any]
    primitive: Dict
    mesh_kwargs: Dict
    accessors: List


class PrimitivePreprocessContext(TypedDict):
    """Context for primitive_preprocess scope handlers (pre-load)."""

    data: Dict[str, Any]
    primitive: Dict
    accessors: List
    views: List


class PrimitiveExportContext(TypedDict):
    """Context for primitive_export scope handlers (during export)."""

    mesh: Any
    name: str
    tree: Dict
    buffer_items: OrderedDict
    primitive: Dict
    include_normals: bool


# Handler type alias - handlers receive a context dict
Handler = Callable[[Any], Any]

# callback to parse material dict and resolve texture references
# signature: (*, data: Dict) -> Dict
ParseTextures = Callable[..., Dict[str, Any]]

# Registry: {scope: {extension_name: handler}}
_handlers: Dict[str, Dict[str, Handler]] = {}


def _deep_merge(target: Dict, source: Dict) -> None:
    """
    Recursively merge source dict into target dict.

    Parameters
    ----------
    target
      Dict to merge into (modified in place)
    source
      Dict to merge from
    """
    for key, value in source.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            # Both are dicts - recurse
            _deep_merge(target[key], value)
        else:
            # Overwrite or set new key
            target[key] = value


def register_handler(name: str, scope: Scope) -> Callable[[Handler], Handler]:
    """
    Decorator to register a handler for a glTF extension.

    Parameters
    ----------
    name
      Extension name, e.g. "KHR_materials_pbrSpecularGlossiness".
    scope
      Handler scope, e.g. "material", "texture_source", "primitive".

    Returns
    -------
    decorator
      Function that registers the handler and returns it unchanged.

    Example
    -------
    >>> @register_handler("MY_extension", scope="material")
    ... def my_handler(context: MaterialContext) -> Optional[Dict]:
    ...     data = context["data"]
    ...     images = context["images"]
    ...     return {"baseColorFactor": [1, 0, 0, 1]}
    """
    if scope not in _handlers:
        _handlers[scope] = {}

    def decorator(func: Handler) -> Handler:
        _handlers[scope][name] = func
        return func

    return decorator


def handle_extensions(
    *,
    extensions: Optional[Dict[str, Any]],
    scope: Scope,
    **kwargs,
) -> Any:
    """
    Process extensions dict for a given scope, calling registered handlers.

    Parameters
    ----------
    extensions
      The "extensions" dict from a glTF element, or None.
    scope
      Handler scope to invoke.
    **kwargs
      Scope-specific arguments that will be combined with extension data
      into a typed context dict. Required kwargs by scope:
        - material: parse_textures, images
        - texture_source: (none)
        - primitive: primitive, mesh_kwargs, accessors
        - primitive_preprocess: primitive, accessors, views
        - primitive_export: mesh, name, tree, buffer_items, primitive, include_normals

    Returns
    -------
    results
      Dict of {extension_name: result} for most scopes.
      For scopes ending in "_source", returns first non-None result.
      For "primitive" scope, automatically merges results into mesh_kwargs.
    """
    if not extensions or scope not in _handlers:
        return {} if not scope.endswith("_source") else None

    results = {}
    for ext_name, data in extensions.items():
        if ext_name not in _handlers[scope]:
            continue
        try:
            # Build context dict with data + all kwargs
            context = {"data": data, **kwargs}
            if (result := _handlers[scope][ext_name](context)) is not None:
                results[ext_name] = result
        except Exception as e:
            log.warning(f"failed to process extension {ext_name}: {e}")

    # for _source scopes return first result, otherwise return all results
    if scope.endswith("_source"):
        return next(iter(results.values()), None)

    # for primitive scope, automatically merge results into mesh_kwargs
    if scope == "primitive" and "mesh_kwargs" in kwargs:
        mesh_kwargs = kwargs["mesh_kwargs"]
        for ext_result in results.values():
            if not isinstance(ext_result, dict):
                continue
            # merge extension results, recursively merging nested dicts
            for key, value in ext_result.items():
                if isinstance(value, dict):
                    if key not in mesh_kwargs:
                        mesh_kwargs[key] = {}
                    _deep_merge(mesh_kwargs[key], value)
                else:
                    mesh_kwargs[key] = value

    return results


# ----------------------------------------------------------------------
# Built-in handlers
# ----------------------------------------------------------------------


@register_handler("KHR_materials_pbrSpecularGlossiness", scope="material")
def _specular_glossiness(context: MaterialContext) -> Optional[Dict[str, Any]]:
    """
    Convert specular-glossiness material to PBR metallic-roughness.

    Parameters
    ----------
    context
      MaterialContext with extension data, parse_textures function, and images.

    Returns
    -------
    pbr_dict
      PBR metallic-roughness parameters, or None on failure.
    """
    try:
        from ...visual.gloss import specular_to_pbr

        return specular_to_pbr(**context["parse_textures"](data=context["data"]))
    except Exception:
        log.debug("failed to convert specular-glossiness", exc_info=True)
        return None


@register_handler("EXT_texture_webp", scope="texture_source")
def _texture_webp_source(context: TextureSourceContext) -> Optional[int]:
    """
    Return image source index from EXT_texture_webp.

    Parameters
    ----------
    context
      TextureSourceContext with extension data.

    Returns
    -------
    source_index
      Index into glTF images array, or None if not present.
    """
    return context["data"].get("source")
