"""
Defines the base constance backend
"""


class Backend:

    def get(self, key):
        """
        Get the key from the backend store and return the value.
        Return None if not found.
        """
        raise NotImplementedError

    def mget(self, keys):
        """
        Get the keys from the backend store and return a list of the values.
        Return an empty list if not found.
        """
        raise NotImplementedError

    def set(self, key, value):
        """
        Add the value to the backend store given the key.
        """
        raise NotImplementedError
