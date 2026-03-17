def clear_builtins(attrs):
    """
    Clears the builtins from an ``attrs`` dict

    Returns a new dict without the builtins

    """
    new_attrs = {}

    for key in attrs.keys():
        if not(key.startswith('__') and key.endswith('__')):
            new_attrs[key] = attrs[key]

    return new_attrs
