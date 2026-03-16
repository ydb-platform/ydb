# encoding: utf-8

"""Objects shared by modules in the pptx.opc sub-package."""


class CaseInsensitiveDict(dict):
    """Mapping type like dict except it matches key without respect to case.

    For example, D['A'] == D['a']. Note this is not general-purpose, just complete
    enough to satisfy opc package needs. It assumes str keys for example.
    """

    def __contains__(self, key):
        return super(CaseInsensitiveDict, self).__contains__(key.lower())

    def __getitem__(self, key):
        return super(CaseInsensitiveDict, self).__getitem__(key.lower())

    def __setitem__(self, key, value):
        return super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)
