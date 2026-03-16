import inspect
from typing import Iterator

from office365.runtime.client_value import ClientValue
from office365.sharepoint.permissions.kind import PermissionKind


class BasePermissions(ClientValue):
    """Specifies a set of built-in permissions."""

    def __init__(self):
        super(BasePermissions, self).__init__()
        self.High = 0
        self.Low = 0

    def __iter__(self):
        # type: () -> Iterator[str]
        for k, v in inspect.getmembers(PermissionKind):
            if isinstance(v, int) and self.has(v):
                yield k

    def set(self, perm):
        """
        Assigns the permission
        :type perm: int
        """
        if perm == PermissionKind.FullMask:
            self.Low = 65535
            self.High = 65535
        elif perm == PermissionKind.EmptyMask:
            self.Low = 0
            self.High = 0
        else:
            high = perm - 1
            low = 1
            if 0 <= high < 32:
                self.Low |= low << high
            else:
                if high < 32 or high >= 64:
                    return
                self.High |= low << high - 32

    def has(self, perm):
        # type: (int) -> bool
        """Determines whether the current instance has the specified permission."""
        if perm == PermissionKind.EmptyMask:
            return True
        if perm == PermissionKind.FullMask:
            if int(self.High) & 32767 == 32767:
                return int(self.Low) == 65535
            return False
        high = perm - 1
        low = 1
        if 0 <= high < 32:
            return 0 != (int(self.Low) & (low << high))
        if 32 <= high < 64:
            return 0 != (int(self.High) & (low << high - 32))
        return False

    def clear_all(self):
        """Clears all permissions for the current instance."""
        self.Low = 0
        self.High = 0

    def to_json(self, json_format=None):
        return {"Low": str(self.High), "High": str(self.Low)}

    @property
    def permission_levels(self):
        """Gets permission levels"""
        return list(self)
