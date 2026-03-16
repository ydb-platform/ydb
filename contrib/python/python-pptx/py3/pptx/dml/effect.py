"""Visual effects on a shape such as shadow, glow, and reflection."""

from __future__ import annotations


class ShadowFormat(object):
    """Provides access to shadow effect on a shape."""

    def __init__(self, spPr):
        # ---spPr may also be a grpSpPr; both have a:effectLst child---
        self._element = spPr

    @property
    def inherit(self):
        """True if shape inherits shadow settings.

        Read/write. An explicitly-defined shadow setting on a shape causes
        this property to return |False|. A shape with no explicitly-defined
        shadow setting inherits its shadow settings from the style hierarchy
        (and so returns |True|).

        Assigning |True| causes any explicitly-defined shadow setting to be
        removed and inheritance is restored. Note this has the side-effect of
        removing **all** explicitly-defined effects, such as glow and
        reflection, and restoring inheritance for all effects on the shape.
        Assigning |False| causes the inheritance link to be broken and **no**
        effects to appear on the shape.
        """
        if self._element.effectLst is None:
            return True
        return False

    @inherit.setter
    def inherit(self, value):
        inherit = bool(value)
        if inherit:
            # ---remove any explicitly-defined effects
            self._element._remove_effectLst()
        else:
            # ---ensure at least the effectLst element is present
            self._element.get_or_add_effectLst()
