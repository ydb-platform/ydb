"""
attrdict contains several mapping objects that allow access to their
keys as attributes.
"""
from attrdict.mapping import AttrMap
from attrdict.dictionary import AttrDict
from attrdict.default import AttrDefault


__all__ = ['AttrMap', 'AttrDict', 'AttrDefault']
