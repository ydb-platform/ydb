def with_metaclass(meta, base=object):
    return meta("NewBase", (base,), {})
