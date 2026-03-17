from __future__ import division


def get_aslist(param):
    """Checks if parameter is described as list for simpler scenarios"""
    # if schema is not defined it's a complex scenario
    if 'schema' not in param:
        return False

    param_schema = param / 'schema'
    schema_type = param_schema.getkey('type', 'any')
    # TODO: resolve for 'any' schema type
    return schema_type in ['array', 'object']


def get_style(param):
    """Checks parameter style for simpler scenarios"""
    if 'style' in param:
        return param['style']

    # determine default
    return (
        'simple' if param['in'] in ['path', 'header'] else 'form'
    )


def get_explode(param):
    """Checks parameter explode for simpler scenarios"""
    if 'explode' in param:
        return param['explode']

    # determine default
    style = get_style(param)
    return style == 'form'
