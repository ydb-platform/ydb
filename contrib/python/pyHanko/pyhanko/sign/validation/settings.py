from dataclasses import dataclass
from typing import Optional, Set

from asn1crypto import x509
from pyhanko_certvalidator.errors import InvalidCertificateError

from pyhanko.config.api import (
    ConfigurableMixin,
    process_bit_string_flags,
    process_oids,
)


def _match_usages(required: set, present: set, need_all: bool):
    if need_all:
        return not (required - present)
    else:
        # intersection must be non-empty
        return bool(required & present)


@dataclass(frozen=True)
class KeyUsageConstraints(ConfigurableMixin):
    """
    Convenience class to pass around key usage requirements and validate them.
    Intended to be flexible enough to handle both PKIX and ISO 32000 certificate
    seed value constraint semantics.

    .. versionchanged:: 0.6.0
        Bring extended key usage semantics in line with :rfc:`5280` (PKIX).
    """

    key_usage: Optional[Set[str]] = None
    """
    All or some (depending on :attr:`match_all_key_usage`) of these key usage
    extensions must be present in the signer's certificate.
    If not set or empty, all key usages are considered acceptable.
    """

    key_usage_forbidden: Optional[Set[str]] = None
    """
    These key usages must not be present in the signer's certificate.

    .. note::
        This behaviour is undefined in :rfc:`5280` (PKIX), but included for
        compatibility with certificate seed value settings in ISO 32000.
    """

    extd_key_usage: Optional[Set[str]] = None
    """
    List of acceptable key purposes that can appear in an extended key
    usage extension in the signer's certificate, if such an extension is at all
    present. If not set, all extended key usages are considered acceptable.

    If no extended key usage extension is present, or if the
    ``anyExtendedKeyUsage`` key purpose ID is present, the resulting behaviour
    depends on :attr:`explicit_extd_key_usage_required`.

    Setting this option to the empty set (as opposed to ``None``) effectively
    bans all (presumably unrecognised) extended key usages.

    .. warning::
        Note the difference in behaviour with :attr:`key_usage` for empty
        sets of valid usages.

    .. warning::
        Contrary to what some CAs seem to believe, the criticality of the
        extended key usage extension is irrelevant here.
        Even a non-critical EKU extension **must** be enforced according to
        :rfc:`5280` § 4.2.1.12.

        In practice, many certificate authorities issue non-repudiation certs
        that can also be used for TLS authentication by only including the
        TLS client authentication key purpose ID in the EKU extension.
        Interpreted strictly, :rfc:`5280` bans such certificates from being
        used to sign documents, and pyHanko will enforce these semantics
        if :attr:`extd_key_usage` is not ``None``.
    """

    explicit_extd_key_usage_required: bool = True
    """
    .. versionadded:: 0.6.0

    Require an extended key usage extension with the right key usages to be
    present if :attr:`extd_key_usage` is non-empty.

    If this flag is ``True``, at least one key purpose in :attr:`extd_key_usage`
    must appear in the certificate's extended key usage, and
    ``anyExtendedKeyUsage`` will be ignored.
    """

    match_all_key_usages: bool = False
    """
    .. versionadded:: 0.6.0

    If ``True``, all key usages indicated in :attr:`key_usage` must be present
    in the certificate. If ``False``, one match suffices.

    If :attr:`key_usage` is empty or ``None``, this option has no effect.
    """

    def validate(self, cert: x509.Certificate):
        self._validate_key_usage(cert.key_usage_value)
        self._validate_extd_key_usage(cert.extended_key_usage_value)

    def _validate_key_usage(self, key_usage_extension_value):
        if not self.key_usage:
            return
        key_usage = self.key_usage or set()
        key_usage_forbidden = self.key_usage_forbidden or set()

        # First, check the "regular" key usage extension
        cert_ku = (
            set(key_usage_extension_value.native)
            if key_usage_extension_value is not None
            else set()
        )

        # check blacklisted key usages (ISO 32k)
        forbidden_ku = cert_ku & key_usage_forbidden
        if forbidden_ku:
            rephrased = map(lambda s: s.replace('_', ' '), forbidden_ku)
            raise InvalidCertificateError(
                "The active key usage policy explicitly bans certificates "
                f"used for {', '.join(rephrased)}."
            )

        # check required key usage extension values
        need_all_ku = self.match_all_key_usages
        if not _match_usages(key_usage, cert_ku, need_all_ku):
            rephrased = map(lambda s: s.replace('_', ' '), key_usage)
            raise InvalidCertificateError(
                "The active key usage policy requires "
                f"{'' if need_all_ku else 'at least one of '}the key "
                f"usage extensions {', '.join(rephrased)} to be present."
            )

    def _validate_extd_key_usage(self, eku_extension_value):
        if self.extd_key_usage is None:
            return
        # check extended key usage
        has_extd_key_usage_ext = eku_extension_value is not None
        cert_eku = (
            set(eku_extension_value.native) if has_extd_key_usage_ext else set()
        )

        if (
            'any_extended_key_usage' in cert_eku
            and not self.explicit_extd_key_usage_required
        ):
            return  # early out, cert is valid for all EKUs

        extd_key_usage = self.extd_key_usage or set()
        if not has_extd_key_usage_ext:
            if self.explicit_extd_key_usage_required:
                raise InvalidCertificateError(
                    "The active key usage policy requires an extended "
                    "key usage extension."
                )
            return  # early out, cert is (presumably?) valid for all EKUs

        if not _match_usages(extd_key_usage, cert_eku, need_all=False):
            if extd_key_usage:
                rephrased = map(lambda s: s.replace('_', ' '), extd_key_usage)
                ok_list = f"Relevant key purposes are {', '.join(rephrased)}."
            else:
                ok_list = "There are no acceptable extended key usages."
            raise InvalidCertificateError(
                "The extended key usages for which this certificate is valid "
                f"do not match the active key usage policy. {ok_list}"
            )

    @classmethod
    def process_entries(cls, config_dict):
        super().process_entries(config_dict)

        # Deal with KeyUsage values first
        # might as well expose key_usage_forbidden while we're at it
        for key_usage_sett in ('key_usage', 'key_usage_forbidden'):
            affected_flags = config_dict.get(key_usage_sett, None)
            if affected_flags is not None:
                config_dict[key_usage_sett] = set(
                    process_bit_string_flags(
                        x509.KeyUsage,
                        affected_flags,
                        key_usage_sett.replace('_', '-'),
                    )
                )

        extd_key_usage = config_dict.get('extd_key_usage', None)
        if extd_key_usage is not None:
            config_dict['extd_key_usage'] = set(
                process_oids(
                    x509.KeyPurposeId, extd_key_usage, 'extd-key-usage'
                )
            )
