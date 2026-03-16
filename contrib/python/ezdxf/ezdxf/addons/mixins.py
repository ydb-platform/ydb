# Copyright (c) 2011, Manfred Moitzi
# License: MIT License


class SubscriptAttributes:
    def __getitem__(self, item):
        if hasattr(self, item):
            return getattr(self, item)
        else:
            raise KeyError(item)
        
    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(key)

    def __contains__(self, item):
        return hasattr(self, item)
