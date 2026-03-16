from __future__ import division

from six import iteritems


def get_all_properties(schema):
    properties = schema.get('properties', {})
    properties_dict = dict(iteritems(properties))

    if 'allOf'not in schema:
        return properties_dict

    for subschema in schema / 'allOf':
        subschema_props = get_all_properties(subschema)
        properties_dict.update(subschema_props)

    return properties_dict


def get_all_properties_names(schema):
    all_properties = get_all_properties(schema)
    return set(all_properties.keys())
