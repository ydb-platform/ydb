def is_attribute_overridden(base: type, cls: type, attribute_name: str) -> bool:
    """Return True if attribute is overridden by any ancestor before reaching base in the MRO."""
    for ancestor in cls.mro():
        if ancestor is base:
            return False
        if attribute_name in ancestor.__dict__:
            return True
    return False
