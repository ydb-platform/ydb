# coding: utf-8
from datetime import datetime
from typing import List, Optional, Type, TypeVar

from asn1crypto.crl import CRLReason
from cryptography.exceptions import InvalidSignature
from pyhanko_certvalidator._state import ValProcState
from pyhanko_certvalidator.path import ValidationPath


class PathError(Exception):
    pass


class PathBuildingError(PathError):
    pass


class CertificateFetchError(PathBuildingError):
    pass


class CRLValidationError(Exception):
    pass


class CRLNoMatchesError(CRLValidationError):
    pass


class CRLFetchError(CRLValidationError):
    pass


class CRLValidationIndeterminateError(CRLValidationError):
    def __init__(
        self,
        msg: str,
        failures: List[str],
        suspect_stale: Optional[datetime] = None,
    ):
        self.msg = msg
        self.failures = failures
        self.suspect_stale = suspect_stale
        super().__init__(msg, failures)


class OCSPValidationError(Exception):
    pass


class OCSPNoMatchesError(OCSPValidationError):
    pass


class OCSPValidationIndeterminateError(OCSPValidationError):
    def __init__(
        self,
        msg: str,
        failures: List[str],
        suspect_stale: Optional[datetime] = None,
    ):
        self.msg = msg
        self.failures = failures
        self.suspect_stale = suspect_stale
        super().__init__(msg, failures)


class OCSPFetchError(OCSPValidationError):
    pass


class ValidationError(Exception):
    def __init__(self, message: str):
        self.failure_msg = message
        super().__init__(message)


TPathErr = TypeVar('TPathErr', bound='PathValidationError')


class PathValidationError(ValidationError):
    @classmethod
    def from_state(
        cls: Type[TPathErr], msg: str, proc_state: ValProcState
    ) -> TPathErr:
        return cls(msg, proc_state=proc_state)

    def __init__(self, msg: str, *, proc_state: ValProcState):
        self.is_ee_cert = proc_state.is_ee_cert
        self.is_side_validation = proc_state.is_side_validation
        current = proc_state.cert_path_stack.head
        orig = proc_state.cert_path_stack.last
        assert current is not None and orig is not None
        self.current_path: ValidationPath = current
        self.original_path: ValidationPath = orig
        super().__init__(msg)


class RevokedError(PathValidationError):
    @classmethod
    def format(
        cls,
        reason: CRLReason,
        revocation_dt: datetime,
        revinfo_type: str,
        proc_state: ValProcState,
    ):
        reason_str = reason.human_friendly
        date = revocation_dt.strftime('%Y-%m-%d')
        time = revocation_dt.strftime('%H:%M:%S')
        msg = (
            f'{revinfo_type} indicates {proc_state.describe_cert()} '
            f'was revoked at {time} on {date}, due to {reason_str}.'
        )
        return RevokedError(msg, reason, revocation_dt, proc_state)

    def __init__(
        self,
        msg,
        reason: CRLReason,
        revocation_dt: datetime,
        proc_state: ValProcState,
    ):
        self.reason = reason
        self.revocation_dt = revocation_dt
        super().__init__(msg, proc_state=proc_state)


class InsufficientRevinfoError(PathValidationError):
    pass


class StaleRevinfoError(InsufficientRevinfoError):
    @classmethod
    def format(
        cls,
        msg: str,
        time_cutoff: datetime,
        proc_state: ValProcState,
    ):
        return StaleRevinfoError(msg, time_cutoff, proc_state)

    def __init__(
        self, msg: str, time_cutoff: datetime, proc_state: ValProcState
    ):
        self.time_cutoff = time_cutoff
        super().__init__(msg, proc_state=proc_state)


class InsufficientPOEError(PathValidationError):
    pass


class ExpiredError(PathValidationError):
    @classmethod
    def format(
        cls,
        *,
        expired_dt: datetime,
        proc_state: ValProcState,
    ):
        msg = (
            f"The path could not be validated because "
            f"{proc_state.describe_cert()} expired "
            f"{expired_dt.strftime('%Y-%m-%d %H:%M:%SZ')}"
        )
        return ExpiredError(msg, expired_dt, proc_state)

    def __init__(self, msg, expired_dt: datetime, proc_state: ValProcState):
        self.expired_dt = expired_dt
        super().__init__(msg, proc_state=proc_state)


class NotYetValidError(PathValidationError):
    @classmethod
    def format(
        cls,
        *,
        valid_from: datetime,
        proc_state: ValProcState,
    ):
        msg = (
            f"The path could not be validated because "
            f"{proc_state.describe_cert()} is not valid until "
            f"{valid_from.strftime('%Y-%m-%d %H:%M:%SZ')}"
        )
        return NotYetValidError(msg, valid_from, proc_state)

    def __init__(self, msg, valid_from: datetime, proc_state: ValProcState):
        self.valid_from = valid_from
        super().__init__(msg, proc_state=proc_state)


class InvalidCertificateError(ValidationError):
    pass


class DisallowedAlgorithmError(PathValidationError):
    def __init__(
        self, *args, banned_since: Optional[datetime] = None, **kwargs
    ):
        self.banned_since = banned_since
        super().__init__(*args, **kwargs)

    @classmethod
    def from_state(
        cls,
        msg: str,
        proc_state: ValProcState,
        banned_since: Optional[datetime] = None,
    ) -> 'DisallowedAlgorithmError':
        return cls(msg, banned_since=banned_since, proc_state=proc_state)


class InvalidAttrCertificateError(InvalidCertificateError):
    pass


class PSSParameterMismatch(InvalidSignature):
    pass


class DSAParametersUnavailable(InvalidSignature):
    # TODO Technically, such a signature isn't _really_ invalid
    #  (we merely couldn't validate it).
    # However, this is only an issue for CRLs and OCSP responses that
    # make use of DSA parameter inheritance, which is pretty much a
    # completely irrelevant problem in this day and age, so treating those
    # signatures as invalid as a matter of course seems pretty much OK.
    pass
