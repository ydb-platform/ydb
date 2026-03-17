from typing import Optional, Type

from asn1crypto.algos import DigestAlgorithmId
from asn1crypto.core import ObjectIdentifier

from ..ades.report import AdESIndeterminate, AdESStatus, AdESSubIndic
from ..general import ValueErrorWithMessage

__all__ = [
    'SignatureValidationError',
    'DisallowedAlgorithmError',
    'ValidationInfoReadingError',
    'NoDSSFoundError',
    'SigSeedValueValidationError',
    'CMSAlgorithmProtectionError',
]


class ValidationInfoReadingError(ValueErrorWithMessage):
    """Error reading validation info."""

    pass


class NoDSSFoundError(ValidationInfoReadingError):
    def __init__(self):
        super().__init__("No DSS found")


class CMSAlgorithmProtectionError(ValueErrorWithMessage):
    """Error related to CMS algorithm protection checks."""


class SignatureValidationError(ValueErrorWithMessage):
    """Error validating a signature."""

    def __init__(
        self, failure_message, ades_subindication: Optional[AdESSubIndic] = None
    ):
        self.ades_subindication = ades_subindication
        if ades_subindication:
            msg = "%s [%s]" % (failure_message, ades_subindication)
        else:
            msg = failure_message
        super().__init__(msg)

    @property
    def ades_status(self) -> Optional[AdESStatus]:
        if self.ades_subindication is not None:
            return self.ades_subindication.status
        return None


class DisallowedAlgorithmError(SignatureValidationError):
    def __init__(
        self,
        failure_message,
        permanent: bool,
        oid_type: Optional[Type[ObjectIdentifier]] = None,
    ):
        self.oid_type = oid_type
        if permanent:
            subindic = AdESIndeterminate.CRYPTO_CONSTRAINTS_FAILURE
        else:
            subindic = AdESIndeterminate.CRYPTO_CONSTRAINTS_FAILURE_NO_POE
        super().__init__(
            failure_message=failure_message, ades_subindication=subindic
        )


class SigSeedValueValidationError(SignatureValidationError):
    """Error validating a signature's seed value constraints."""

    # TODO perhaps we can encode some more metadata here, such as the
    #  seed value that tripped the failure.
    pass
