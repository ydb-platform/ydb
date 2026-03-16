import enum
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Union

from asn1crypto import core, x509
from pyhanko_certvalidator.revinfo.archival import CRLContainer, OCSPContainer

__all__ = [
    'ValidationObjectType',
    'ValidationObject',
    'POEType',
    'KnownPOE',
    'POEManager',
    'digest_for_poe',
]


@enum.unique
class ValidationObjectType(enum.Enum):
    """
    Types of validation objects recognised by ETSI TS 119 102-2.
    """

    CERTIFICATE = 'certificate'
    CRL = 'CRL'
    OCSP_RESPONSE = 'OCSPResponse'
    TIMESTAMP = 'timestamp'
    EVIDENCE_RECORD = 'evidencerecord'
    PUBLIC_KEY = 'publicKey'
    SIGNED_DATA = 'signedData'
    OTHER = 'other'

    def urn(self):
        return f'urn:etsi:019102:validationObject:{self.value}'


KnownObjectType = Union[bytes, CRLContainer, OCSPContainer, x509.Certificate]


def guess_validation_object_type(
    thing: object,
) -> Optional[ValidationObjectType]:
    if isinstance(thing, CRLContainer):
        return ValidationObjectType.CRL
    elif isinstance(thing, OCSPContainer):
        return ValidationObjectType.OCSP_RESPONSE
    elif isinstance(thing, x509.Certificate):
        return ValidationObjectType.CERTIFICATE
    return None


@dataclass(frozen=True)
class ValidationObject:
    """
    A validation object used in the course of a validation operation
    for which proofs of existence can potentially be gathered.
    """

    object_type: ValidationObjectType
    """
    The type of validation object.
    """

    value: Any
    """
    The actual object.

    Currently, the following types are supported explicitly.
    Others must currently be supplied as :class:`bytes`.

     - :class:`.CRLContainer`: :attr:`.ValidationObjectType.CRL`
     - :class:`.OCSPContainer`: :attr:`.ValidationObjectType.OCSP_RESPONSE`
     - :class:`x509.Certificate`: :attr:`.ValidationObjectType.CERTIFICATE`
    """


@enum.unique
class POEType(enum.Enum):
    PROVIDED = 'provided'
    VALIDATION = 'validation'
    POLICY = 'policy'

    @property
    def urn(self) -> str:
        return f'urn:etsi:019102:poetype:{self.value}'


@dataclass(frozen=True)
class KnownPOE:
    poe_type: POEType
    digest: bytes
    poe_time: datetime
    validation_object: Optional[ValidationObject] = None


def digest_for_poe(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


class POEManager:
    """
    Class to manage proof-of-existence (POE) claims.

    :param current_dt_override:
        Override the current time.
    """

    def __init__(self, current_dt_override: Optional[datetime] = None):
        self._poes: Dict[bytes, KnownPOE] = {}
        self._current_dt_override = current_dt_override

    def register(
        self,
        data: KnownObjectType,
        poe_type: POEType,
        dt: Optional[datetime] = None,
    ) -> KnownPOE:
        """
        Register a new POE claim if no POE for an earlier time is available.

        :param data:
            Data to register a POE claim for.
        :param poe_type:
            The type of POE.
        :param dt:
            The POE time to register. If ``None``, assume the current time.
        :return:
            The oldest POE datetime available.
        """
        if isinstance(data, bytes):
            b_data = data
        elif isinstance(data, core.Asn1Value):
            b_data = data.dump()
        elif isinstance(data, CRLContainer):
            b_data = data.crl_data.dump()
        elif isinstance(data, OCSPContainer):
            b_data = data.ocsp_response_data.dump()
        else:
            raise NotImplementedError
        digest = digest_for_poe(b_data)

        dt = dt or self._current_dt_override or datetime.now(timezone.utc)
        vo_type = guess_validation_object_type(data)
        vo = None
        if vo_type:
            vo = ValidationObject(object_type=vo_type, value=data)
        return self.register_known_poe(
            KnownPOE(
                poe_type=poe_type,
                digest=digest,
                poe_time=dt,
                validation_object=vo,
            )
        )

    def register_by_digest(
        self,
        digest: bytes,
        poe_type: POEType,
        dt: Optional[datetime] = None,
    ) -> KnownPOE:
        """
        Register a new POE claim if no POE for an earlier time is available.

        :param digest:
            SHA-256 digest of the data to register a POE claim for.
        :param dt:
            The POE time to register. If ``None``, assume the current time.
        :param poe_type:
            The type of POE.
        :return:
            The oldest POE datetime available.
        """
        dt = dt or self._current_dt_override or datetime.now(timezone.utc)
        return self.register_known_poe(
            KnownPOE(
                poe_type=poe_type,
                digest=digest,
                poe_time=dt,
                validation_object=None,
            )
        )

    def register_known_poe(self, known_poe: KnownPOE) -> KnownPOE:
        """
        Register a new POE claim if no POE for an earlier time is available.

        :param known_poe:
            The POE object to register.
        :return:
            The oldest POE for the given digest.
        """
        dt = known_poe.poe_time
        digest = known_poe.digest
        try:
            cur_poe = self._poes[digest]
            if cur_poe.poe_time <= dt:
                return cur_poe
        except KeyError:
            pass
        self._poes[digest] = known_poe
        return known_poe

    def __iter__(self) -> Iterator[KnownPOE]:
        """
        Iterate over the current earliest known POE for all items currently
        being managed.

        Returns an iterator with :class:`KnownPOE` objects.
        """
        return iter(self._poes.values())

    def __getitem__(self, item: KnownObjectType) -> datetime:
        """
        Return the earliest available POE for an item.

        .. note::
            This is a wrapper around :meth:`register` with `dt=None`, and hence
            will register the current time as the POE time for the given item.
            This side effect is intentional.

        :param item:
            Item to get the current POE time for.
        :return:
            A datetime object representing the earliest available POE for the
            item.
        """
        return self.register(
            item, poe_type=POEType.VALIDATION, dt=None
        ).poe_time

    def __ior__(self, other):
        """
        Combine data in another POE manager with the POEs managed by this
        instance.
        """
        if not isinstance(other, POEManager):
            raise TypeError
        for poe in iter(other):
            self.register_known_poe(poe)

    def __copy__(self):
        new_instance = POEManager(current_dt_override=self._current_dt_override)
        new_instance._poes = dict(self._poes)
        return new_instance
