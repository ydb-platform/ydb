import abc
from dataclasses import dataclass
from typing import Dict, Type

from pyhanko.pdf_utils import misc

# TODO Consider maybe allowing a credential exporter that just dumps
#  the file encryption key, and would be compatible with any of the "built-in"
#  security handlers.


@dataclass(frozen=True)
class SerialisedCredential:
    """
    A credential in serialised form.
    """

    credential_type: str
    """
    The registered type name of the credential
    (see :meth:`.SerialisableCredential.register`).
    """

    data: bytes
    """
    The credential data, as a byte string.
    """


class SerialisableCredential(abc.ABC):
    """
    Class representing a credential that can be serialised.
    """

    __registered_subclasses: Dict[str, Type['SerialisableCredential']] = dict()

    @classmethod
    def get_name(cls) -> str:
        """
        Get the type name of the credential, which will be embedded into
        serialised values and used on deserialisation.
        """
        raise NotImplementedError

    @staticmethod
    def register(cls: Type['SerialisableCredential']):
        """
        Register a subclass into the credential serialisation registry, using
        the name returned by :meth:`get_name`. Can be used as a class decorator.

        :param cls:
            The subclass.
        :return:
            The subclass.
        """
        SerialisableCredential.__registered_subclasses[cls.get_name()] = cls
        return cls

    def _ser_value(self) -> bytes:
        """
        Serialise a value to raw binary data. To be overridden by subclasses.

        :return:
            A byte string
        :raises misc.PdfWriteError:
            If a serialisation error occurs.
        """
        raise NotImplementedError

    @classmethod
    def _deser_value(cls, data: bytes):
        """
        Deserialise a value from raw binary data.

        :param data:
            The data to deserialise.
        :return:
            The deserialised value (an instance of a subclass of
            :class:`.SerialisableCredential`)
        :raises misc.PdfReadError:
            If a deserialisation error occurs.
        """

        raise NotImplementedError

    @staticmethod
    def deserialise(
        ser_value: SerialisedCredential,
    ) -> 'SerialisableCredential':
        """
        Deserialise a :class:`.SerialisedCredential` value by looking up
        the proper subclass of :class:`.SerialisableCredential` and invoking
        its deserialisation method.

        :param ser_value:
            The value to deserialise.
        :return:
            The deserialised credential.
        :raises misc.PdfReadError:
            If a deserialisation error occurs.
        """
        cred_type = ser_value.credential_type
        try:
            cls = SerialisableCredential.__registered_subclasses[cred_type]
        except KeyError:
            raise misc.PdfReadError(
                f"Failed to deserialise credential: "
                f"credential type '{cred_type}' not known."
            )
        return cls._deser_value(ser_value.data)

    def serialise(self) -> SerialisedCredential:
        """
        Serialise a value to an annotated :class:`.SerialisedCredential` value.

        :return:
            A :class:`.SerialisedCredential` value.
        :raises misc.PdfWriteError:
            If a serialisation error occurs.
        """
        return SerialisedCredential(
            credential_type=self.__class__.get_name(), data=self._ser_value()
        )
