import binascii
import enum
import warnings
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Set, Union

from asn1crypto import x509

from pyhanko.config import api
from pyhanko.config.errors import ConfigurationError
from pyhanko.keys import load_cert_from_pemder, load_certs_from_pemder
from pyhanko.pdf_utils.misc import get_and_apply

__all__ = ['TokenCriteria', 'PKCS11PinEntryMode', 'PKCS11SignatureConfig']


@dataclass(frozen=True)
class TokenCriteria(api.ConfigurableMixin):
    """
    .. versionadded:: 0.14.0

    Search criteria for a PKCS#11 token.
    """

    label: Optional[str] = None
    """
    Label of the token to use. If ``None``, there is no constraint.
    """

    serial: Optional[bytes] = None
    """
    Serial number of the token to use. If ``None``, there is no constraint.
    """

    @classmethod
    def process_entries(cls, config_dict):
        try:
            config_dict['serial'] = binascii.unhexlify(config_dict['serial'])
        except KeyError:
            pass
        except ValueError as e:
            raise ConfigurationError(
                "Failed to parse PKCS #11 token serial number as a hex string"
            ) from e


class PKCS11PinEntryMode(enum.Enum):
    """
    Pin entry behaviour if the user PIN is not supplied as part of the config.
    """

    PROMPT = enum.auto()
    """
    Prompt for a PIN (the default).

    .. note::
        This value is only processed by the CLI, and ignored when the PKCS#11
        signer is called from library code. In those cases, the default
        is effectively :attr:`SKIP`.
    """

    DEFER = enum.auto()
    """
    Let the PKCS #11 module handle its own authentication during login.

    .. note::
        This applies to some devices that have physical PIN pads, for example.
    """

    SKIP = enum.auto()
    """
    Skip the login process altogether.

    .. note::
        This applies to some devices that manage user authentication outside
        the scope of PKCS #11 entirely.
    """

    @staticmethod
    def parse_mode_setting(value: Any) -> 'PKCS11PinEntryMode':
        if isinstance(value, str):
            try:
                return PKCS11PinEntryMode.__members__[value.upper()]
            except KeyError:
                raise ConfigurationError(
                    f"Invalid PIN entry mode {value!r}; must be one of "
                    f"{', '.join(repr(x.name) for x in PKCS11PinEntryMode)}."
                )
        else:
            # fallback for backwards compatibility
            return (
                PKCS11PinEntryMode.PROMPT if value else PKCS11PinEntryMode.SKIP
            )


@dataclass(frozen=True)
class PKCS11SignatureConfig(api.ConfigurableMixin):
    """
    Configuration for a PKCS#11 signature.

    This class is used to load PKCS#11 setup information from YAML
    configuration.
    """

    module_path: str
    """Path to the PKCS#11 module shared object."""

    cert_label: Optional[str] = None
    """PKCS#11 label of the signer's certificate."""

    cert_id: Optional[bytes] = None
    """PKCS#11 ID of the signer's certificate."""

    signing_certificate: Optional[x509.Certificate] = None
    """
    The signer's certificate. If present, :attr:`cert_id` and
    :attr:`cert_label` will not be used to obtain the signer's certificate
    from the PKCS#11 token.

    .. note::
        This can be useful in case the signer's certificate is not available on
        the token, or if you would like to present a different certificate than
        the one provided on the token.
    """

    token_criteria: Optional[TokenCriteria] = None
    """PKCS#11 token name"""

    other_certs: Optional[List[x509.Certificate]] = None
    """Other relevant certificates."""

    key_label: Optional[str] = None
    """
    PKCS#11 label of the signer's private key. Defaults to :attr:`cert_label`
    if the latter is specified and :attr:`key_id` is not.
    """

    key_id: Optional[bytes] = None
    """
    PKCS#11 key ID.
    """

    slot_no: Optional[int] = None
    """
    Slot number of the PKCS#11 slot to use.
    """

    user_pin: Optional[str] = None
    """
    The user's PIN. If unspecified, the user will be prompted for a PIN
    if :attr:`prompt_pin` is ``True``.

    .. warning::
        Some PKCS#11 tokens do not allow the PIN code to be communicated in
        this way, but manage their own authentication instead (the Belgian eID
        middleware is one such example).
        For such tokens, leave this setting set to ``None`` and additionally
        set :attr:`prompt_pin` to ``False``.
    """

    prompt_pin: PKCS11PinEntryMode = PKCS11PinEntryMode.PROMPT
    """
    Set PIN entry and PKCS #11 login behaviour.

    .. note::
        If :attr:`user_pin` is not ``None``, this setting has no effect.
    """

    other_certs_to_pull: Optional[Iterable[str]] = ()
    """
    List labels of other certificates to pull from the PKCS#11 device.
    Defaults to the empty tuple. If ``None``, pull *all* certificates.
    """

    bulk_fetch: bool = True
    """
    Boolean indicating the fetching strategy.
    If ``True``, fetch all certs and filter the unneeded ones.
    If ``False``, fetch the requested certs one by one.
    Default value is ``True``, unless ``other_certs_to_pull`` has one or
    fewer elements, in which case it is always treated as ``False``.
    """

    prefer_pss: bool = False
    """
    Prefer PSS to PKCS#1 v1.5 padding when creating RSA signatures.
    """

    raw_mechanism: bool = False
    """
    Invoke the raw variant of the PKCS#11 signing operation.

    .. note::
        This is currently only supported for ECDSA signatures.
    """

    @classmethod
    def check_config_keys(cls, keys_supplied: Set[str]):
        # make sure we don't ding token_label since we actually still
        # process it for compatibility reasons
        super().check_config_keys(
            {
                k
                for k in keys_supplied
                if k not in ('token_label', 'token-label')
            }
        )

    @classmethod
    def process_entries(cls, config_dict):
        super().process_entries(config_dict)
        other_certs = config_dict.get('other_certs', ())
        if isinstance(other_certs, str):
            other_certs = (other_certs,)
        config_dict['other_certs'] = list(load_certs_from_pemder(other_certs))

        cert_file = config_dict.get('signing_certificate', None)
        if cert_file is not None:
            config_dict['signing_certificate'] = load_cert_from_pemder(
                cert_file
            )

        if 'key_id' in config_dict:
            config_dict['key_id'] = _process_pkcs11_id_value(
                config_dict['key_id']
            )

        if 'cert_id' in config_dict:
            config_dict['cert_id'] = _process_pkcs11_id_value(
                config_dict['cert_id']
            )

        if 'key_label' not in config_dict and 'key_id' not in config_dict:
            if 'cert_id' not in config_dict and 'cert_label' not in config_dict:
                raise ConfigurationError(
                    "Either 'key_id', 'key_label', 'cert_label' or 'cert_id',"
                    "must be provided in PKCS#11 setup"
                )
            if 'cert_id' in config_dict:
                config_dict['key_id'] = config_dict['cert_id']
            if 'cert_label' in config_dict:
                config_dict['key_label'] = config_dict['cert_label']

        if (
            'cert_label' not in config_dict
            and 'cert_id' not in config_dict
            and 'signing_certificate' not in config_dict
        ):
            if 'key_id' in config_dict:
                config_dict['cert_id'] = config_dict['key_id']
            if 'key_label' in config_dict:
                config_dict['cert_label'] = config_dict['key_label']

        config_dict['prompt_pin'] = get_and_apply(
            config_dict,
            'prompt_pin',
            PKCS11PinEntryMode.parse_mode_setting,
            default=PKCS11PinEntryMode.PROMPT,
        )

        if 'token_label' in config_dict:
            warnings.warn(
                "'token_label' is deprecated, use 'token_criteria.label' "
                "instead",
                DeprecationWarning,
            )
            lbl = config_dict.pop('token_label')
            if 'token_criteria' not in config_dict:
                config_dict['token_criteria'] = {'label': lbl}
            else:
                config_dict['token_criteria'].setdefault('label', lbl)


def _process_pkcs11_id_value(x: Union[str, int]):
    if isinstance(x, int):
        return bytes([x])
    else:
        return binascii.unhexlify(x)
