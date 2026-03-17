from typing import Any, Self, TypeGuard

from typing_extensions import Protocol


class Numcodec(Protocol):
    """
    A protocol that models the ``numcodecs.abc.Codec`` interface.

    This protocol should be considered experimental. Expect the type annotations for ``buf`` and
    ``out`` to narrow in the future.
    """

    codec_id: str

    def encode(self, buf: Any) -> Any:
        """Encode data from ``buf``.

        Parameters
        ----------
        buf : Any
            Data to be encoded.

        Returns
        -------
        enc: Any
            Encoded data.
        """
        ...

    def decode(self, buf: Any, out: Any | None = None) -> Any:
        """
        Decode data in ``buf``.

        Parameters
        ----------
        buf : Any
            Encoded data.
        out : Any
            Writeable buffer to store decoded data. If provided, this buffer must
            be exactly the right size to store the decoded data.

        Returns
        -------
        dec : Any
            Decoded data.
        """
        ...

    def get_config(self) -> Any:
        """
        Return a JSON-serializable configuration dictionary for this
        codec. Must include an ``'id'`` field with the codec identifier.
        """
        ...

    @classmethod
    def from_config(cls, config: Any) -> Self:
        """
        Instantiate a codec from a configuration dictionary.

        Parameters
        ----------
        config : Any
            A configuration dictionary for this codec.
        """
        ...


def _is_numcodec_cls(obj: object) -> TypeGuard[type[Numcodec]]:
    """
    Check if the given object is a class implements the Numcodec protocol.

    The @runtime_checkable decorator does not allow issubclass checks for protocols with non-method
    members (i.e., attributes), so we use this function to manually check for the presence of the
    required attributes and methods on a given object.
    """
    return (
        isinstance(obj, type)
        and hasattr(obj, "codec_id")
        and isinstance(obj.codec_id, str)
        and hasattr(obj, "encode")
        and callable(obj.encode)
        and hasattr(obj, "decode")
        and callable(obj.decode)
        and hasattr(obj, "get_config")
        and callable(obj.get_config)
        and hasattr(obj, "from_config")
        and callable(obj.from_config)
    )


def _is_numcodec(obj: object) -> TypeGuard[Numcodec]:
    """
    Check if the given object implements the Numcodec protocol.

    The @runtime_checkable decorator does not allow issubclass checks for protocols with non-method
    members (i.e., attributes), so we use this function to manually check for the presence of the
    required attributes and methods on a given object.
    """
    return _is_numcodec_cls(type(obj))
