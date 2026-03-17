from fingerprints.fingerprint import fingerprint
from fingerprints.cleanup import clean_entity_prefix
from fingerprints.cleanup import clean_brackets
from fingerprints.cleanup import clean_name_light
from fingerprints.cleanup import clean_name_ascii
from fingerprints.types import remove_types, replace_types

generate = fingerprint

__all__ = [
    "fingerprint",
    "generate",
    "clean_entity_prefix",
    "clean_brackets",
    "clean_name_light",
    "clean_name_ascii",
    "remove_types",
    "replace_types",
]
