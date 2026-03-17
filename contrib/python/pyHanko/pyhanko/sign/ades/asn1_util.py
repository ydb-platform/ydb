from typing import Type

from asn1crypto import cms, core

__all__ = ['as_set_of', 'register_cms_attribute']


def as_set_of(asn1_type: Type):
    return type(
        'SetOf' + asn1_type.__name__, (core.SetOf,), {'_child_spec': asn1_type}
    )


def register_cms_attribute(
    dotted_oid: str, readable_name: str, asn1_type: Type
):
    cms.CMSAttributeType._map[dotted_oid] = readable_name
    cms.CMSAttribute._oid_specs[readable_name] = as_set_of(asn1_type)
