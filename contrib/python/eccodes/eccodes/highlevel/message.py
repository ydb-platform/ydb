import io
from contextlib import contextmanager

import numpy as np

import eccodes

_TYPES_MAP = {
    "float": float,
    "int": int,
    "str": str,
}


@contextmanager
def raise_keyerror(name):
    """Make operations on a key raise a KeyError if not found"""
    try:
        yield
    except (eccodes.KeyValueNotFoundError, eccodes.FunctionNotImplementedError):
        raise KeyError(name)


class Message:
    def __init__(self, handle):
        self._handle = handle

    def __del__(self):
        try:
            eccodes.codes_release(self._handle)
        except Exception:
            pass

    def copy(self):
        """Create a copy of the current message"""
        return self.__class__(eccodes.codes_clone(self._handle))

    def __copy__(self):
        return self.copy()

    def _get(self, name, ktype=None):
        name, sep, stype = name.partition(":")
        if sep and ktype is None:
            try:
                ktype = _TYPES_MAP[stype]
            except KeyError:
                raise ValueError(f"Unknown key type {stype!r}")
        with raise_keyerror(name):
            if eccodes.codes_is_missing(self._handle, name):
                raise KeyError(name)
            if eccodes.codes_get_size(self._handle, name) > 1:
                return eccodes.codes_get_array(self._handle, name, ktype=ktype)
            return eccodes.codes_get(self._handle, name, ktype=ktype)

    def get(self, name, default=None, ktype=None):
        """Get the value of a key

        Parameters
        ----------
        name: str
            Name of the key. Can be suffixed with ":str", ":int", or ":float" to
            request a specific type.
        default: any, optional
            Value if the key is not Found, or ``None`` if not specified.
        ktype: type
            Request a specific type for the value. Overrides the suffix in ``name``"""
        try:
            return self._get(name, ktype=ktype)
        except KeyError:
            return default

    def set(self, *args, check_values: bool = True):
        """If two arguments are given, assumes this takes form of a single key
        value pair and sets the value of the given key. If a dictionary is passed in,
        then sets the values of all keys in the dictionary. Note, ordering
        of the keys is important. Finally, by default, checks if values
        have been set correctly

        Raises
        ------
        TypeError
            If arguments do not take one of the two expected forms
        KeyError
            If the key does not exist
        ValueError
            If the set value of one of the keys is not the expected value
        """
        if isinstance(args[0], str) and len(args) == 2:
            key_values = {args[0]: args[1]}
        elif isinstance(args[0], dict):
            key_values = args[0]
        else:
            raise TypeError(
                "Unsupported argument type. Expects two arguments consisting \
            of key and value pair, or a dictionary of key-value pairs"
            )

        for name, value in key_values.items():
            with raise_keyerror(name):
                if np.ndim(value) > 0:
                    eccodes.codes_set_array(self._handle, name, value)
                else:
                    eccodes.codes_set(self._handle, name, value)

        if check_values:
            # Check values just set
            for name, value in key_values.items():
                if type(value) in _TYPES_MAP.values():
                    saved_value = self.get(f"{name}:{type(value).__name__}")
                else:
                    saved_value = self.get(name)
                if not np.all(saved_value == value):
                    raise ValueError(
                        f"Unexpected retrieved value {saved_value} for key {name}. Expected {value}"
                    )

    def get_array(self, name):
        """Get the value of the given key as an array

        Raises
        ------
        KeyError
            If the key is not set
        """
        with raise_keyerror(name):
            return eccodes.codes_get_array(self._handle, name)

    def get_size(self, name):
        """Get the size of the given key

        Raises
        ------
        KeyError
            If the key is not set
        """
        with raise_keyerror(name):
            return eccodes.codes_get_size(self._handle, name)

    def get_data_points(self):
        raise NotImplementedError

    def is_missing(self, name):
        """Check whether the key is set to a missing value

        Raises
        ------
        KeyError
            If the key is not set
        """
        with raise_keyerror(name):
            return bool(eccodes.codes_is_missing(self._handle, name))

    def set_array(self, name, value):
        """Set the value of the given key

        Raises
        ------
        KeyError
            If the key does not exist
        """
        with raise_keyerror(name):
            return eccodes.codes_set_array(self._handle, name, value)

    def set_missing(self, name):
        """Set the given key as missing

        Raises
        ------
        KeyError
            If the key does not exist
        """
        with raise_keyerror(name):
            return eccodes.codes_set_missing(self._handle, name)

    def __getitem__(self, name):
        return self._get(name)

    def __setitem__(self, name, value):
        self.set(name, value)

    def __contains__(self, name):
        return bool(eccodes.codes_is_defined(self._handle, name))

    class _KeyIterator:
        def __init__(self, message, namespace=None, iter_keys=True, iter_values=False):
            self._message = message
            self._iterator = eccodes.codes_keys_iterator_new(message._handle, namespace)
            self._iter_keys = iter_keys
            self._iter_values = iter_values

        def __del__(self):
            try:
                eccodes.codes_keys_iterator_delete(self._iterator)
            except Exception:
                pass

        def __iter__(self):
            return self

        def __next__(self):
            while True:
                if not eccodes.codes_keys_iterator_next(self._iterator):
                    raise StopIteration
                if not self._iter_keys and not self._iter_values:
                    return
                key = eccodes.codes_keys_iterator_get_name(self._iterator)
                if self._message.is_missing(key):
                    continue
                if self._iter_keys and not self._iter_values:
                    return key
                value = self._message.get(key) if self._iter_values else None
                if not self._iter_keys:
                    return value
                return key, value

    def __iter__(self):
        return self._KeyIterator(self)

    def keys(self, namespace=None):
        """Iterate over all the available keys"""
        return self._KeyIterator(self, namespace, iter_keys=True, iter_values=False)

    def values(self, namespace=None):
        """Iterate over the values of all the available keys"""
        return self._KeyIterator(self, namespace, iter_keys=False, iter_values=True)

    def items(self, namespace=None):
        """Iterate over all the available key-value pairs"""
        return self._KeyIterator(self, namespace, iter_keys=True, iter_values=True)

    def dump(self):
        """Print out a textual representation of the message"""
        eccodes.codes_dump(self._handle)

    def write_to(self, fileobj):
        """Write the message to a file object"""
        assert isinstance(fileobj, io.IOBase)
        eccodes.codes_write(self._handle, fileobj)

    def get_buffer(self):
        """Return a buffer containing the encoded message"""
        return eccodes.codes_get_message(self._handle)


class BUFRMessage(Message):
    def __init__(self, handle):
        super().__init__(handle)

    def pack(self):
        """Pack the underlying data"""
        self.set("pack", 1, check_values=False)

    def unpack(self):
        """Unpack the underlying data"""
        self.set("unpack", 1, check_values=False)

    @classmethod
    def from_samples(cls, name):
        """Create a message from a sample"""
        return cls(eccodes.codes_bufr_new_from_samples(name))


class GRIBMessage(Message):
    def __init__(self, handle):
        super().__init__(handle)
        self._data = None

    @property
    def data(self):
        """Return the array of values"""
        if self._data is None:
            self._data = self._get("values")
        return self._data

    def get_data_points(self):
        """Get the list of ``(lat, lon, value)`` data points"""
        return eccodes.codes_grib_get_data(self._handle)

    @classmethod
    def from_samples(cls, name):
        """Create a message from a sample"""
        return cls(eccodes.codes_grib_new_from_samples(name))
