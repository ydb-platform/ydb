import logging
import pickle
import typing as _t


class BaseSerializer:
    """This is the base interface for all default serializers.

    BaseSerializer.load and BaseSerializer.dump will
    default to pickle.load and pickle.dump. This is currently
    used only by FileSystemCache which dumps/loads to/from a file stream.
    """

    def _warn(self, e: pickle.PickleError) -> None:
        logging.warning(
            f"An exception has been raised during a pickling operation: {e}"
        )

    def dump(
        self, value: int, f: _t.IO, protocol: int = pickle.HIGHEST_PROTOCOL
    ) -> None:
        try:
            pickle.dump(value, f, protocol)
        except (pickle.PickleError, pickle.PicklingError) as e:
            self._warn(e)

    def load(self, f: _t.BinaryIO) -> _t.Any:
        try:
            data = pickle.load(f)
        except pickle.PickleError as e:
            self._warn(e)
            return None
        else:
            return data

    """BaseSerializer.loads and BaseSerializer.dumps
    work on top of pickle.loads and pickle.dumps. Dumping/loading
    strings and byte strings is the default for most cache types.
    """

    def dumps(self, value: _t.Any, protocol: int = pickle.HIGHEST_PROTOCOL) -> bytes:
        try:
            serialized = pickle.dumps(value, protocol)
        except (pickle.PickleError, pickle.PicklingError) as e:
            self._warn(e)
        return serialized

    def loads(self, bvalue: bytes) -> _t.Any:
        try:
            data = pickle.loads(bvalue)
        except pickle.PickleError as e:
            self._warn(e)
            return None
        else:
            return data


"""Default serializers for each cache type.

The following classes can be used to further customize
serialiation behaviour. Alternatively, any serializer can be
overriden in order to use a custom serializer with a different
strategy altogether.
"""


class UWSGISerializer(BaseSerializer):
    """Default serializer for UWSGICache."""


class SimpleSerializer(BaseSerializer):
    """Default serializer for SimpleCache."""


class FileSystemSerializer(BaseSerializer):
    """Default serializer for FileSystemCache."""


class RedisSerializer(BaseSerializer):
    """Default serializer for RedisCache."""

    def dumps(self, value: _t.Any, protocol: int = pickle.HIGHEST_PROTOCOL) -> bytes:
        """Dumps an object into a string for redis, using pickle by default."""
        return b"!" + pickle.dumps(value, protocol)

    def loads(self, value: _t.Optional[bytes]) -> _t.Any:
        """The reversal of :meth:`dump_object`. This might be called with
        None.
        """
        if value is None:
            return None
        if value.startswith(b"!"):
            try:
                return pickle.loads(value[1:])
            except pickle.PickleError:
                return None
        try:
            return int(value)
        except ValueError:
            # before 0.8 we did not have serialization. Still support that.
            return value


class DynamoDbSerializer(RedisSerializer):
    """Default serializer for DynamoDbCache."""

    def loads(self, value: _t.Any) -> _t.Any:
        """The reversal of :meth:`dump_object`. This might be called with
        None.
        """
        value = value.value
        return super().loads(value)
