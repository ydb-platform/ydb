__all__ = ['InfiniteOverlapParametersMixin']
from typing import Optional

import attr


@attr.attrs(these={
    'infinite_overlap': attr.attrib(default=None),
    'overlap': attr.attrib(default=None),
})
class InfiniteOverlapParametersMixin:
    """
    This mixin provides `overlap` and `infinite_overlap` attributes
    and is responsible for maintaining their consistency.

    Possible states:
    * `overlap` is `None` and `infinite_overlap` is `None`:
        Interpreted as "overlap was not provided"
    * `overlap` is `None` and `infinite_overlap` is `True`:
        Interpreted as "infinite overlap"
    * `overlap` is not `None` and `infinite_overlap` is `False`:
        Interpreted as "finite overlap of `overlap`"

    All other states are considered invalid
    """

    _infinite_overlap: Optional[bool]
    _overlap: Optional[int]

    def __attrs_post_init__(self):
        if self._overlap is None:
            if self._infinite_overlap is False:
                raise ValueError('Finite overlap requires an overlap value')
        else:
            if self._infinite_overlap:
                raise ValueError('Infinite overlap requires overlap to be None')
            self._infinite_overlap = False

    def unset_overlap(self):
        """Unsets overlap"""
        self._overlap = None
        self._infinite_overlap = None

    @property
    def overlap(self) -> Optional[int]:
        return self._overlap

    @overlap.setter
    def overlap(self, value: Optional[int]):
        """
        Sets finite overlap via assignment.

        After initialization can be assigned only non-None values.

        During first initialization can be assigned any value,
        overall consistency is checked in __attrs_post_init__.
        """

        # Is this an initialization?
        if not hasattr(self, '_overlap'):
            self._overlap = value
            return

        if value is None:
            raise ValueError('Assign True to `infinite_overlap` to set an infinite overlap')

        self._overlap = value
        self._infinite_overlap = False

    @property
    def infinite_overlap(self) -> Optional[bool]:
        return self._infinite_overlap

    @infinite_overlap.setter
    def infinite_overlap(self, value: Optional[bool]):
        """
        Set infinite overlap via assignment.

        After initialization can be assigned only `True` value.

        During first initialization can be assigned any value,
        overall consistency is checked in __attrs_post_init__.
        """

        # Is this an initialization?
        if not hasattr(self, '_infinite_overlap'):
            self._infinite_overlap = value
            return

        if not value:
            raise ValueError('Assign a value to `overlap` to set a finite overlap')

        self._overlap = None
        self._infinite_overlap = True

    def unstructure(self) -> Optional[dict]:
        """
        Ensures that if either overlap or infinite_overlap is not None, then
        both of them are present in unstructured value.
        """
        data = super().unstructure()
        if self.overlap is not None or self.infinite_overlap is not None:
            data.update({'overlap': self.overlap, 'infinite_overlap': self.infinite_overlap})
        return data
