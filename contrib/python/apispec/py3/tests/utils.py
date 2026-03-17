"""Utilities to get elements of generated spec"""

from apispec.utils import build_reference


def get_schemas(spec):
    if spec.openapi_version.major < 3:
        return spec.to_dict()["definitions"]
    return spec.to_dict()["components"]["schemas"]


def get_responses(spec):
    if spec.openapi_version.major < 3:
        return spec.to_dict()["responses"]
    return spec.to_dict()["components"]["responses"]


def get_parameters(spec):
    if spec.openapi_version.major < 3:
        return spec.to_dict()["parameters"]
    return spec.to_dict()["components"]["parameters"]


def get_examples(spec):
    return spec.to_dict()["components"]["examples"]


def get_security_schemes(spec):
    if spec.openapi_version.major < 3:
        return spec.to_dict()["securityDefinitions"]
    return spec.to_dict()["components"]["securitySchemes"]


def get_paths(spec):
    return spec.to_dict()["paths"]


def build_ref(spec, component_type, obj):
    return build_reference(component_type, spec.openapi_version.major, obj)
