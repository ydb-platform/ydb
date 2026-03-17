from dataclasses import dataclass, field
from typing import Optional

from pyhanko.cli.config import CLIConfig
from pyhanko.sign import PdfSignatureMetadata
from pyhanko.sign.fields import SigFieldSpec
from pyhanko.stamp import BaseStampStyle


@dataclass
class UXContext:
    """
    Context object to track information that affects the UX, e.g. user intent
    as inferred from certain argument combinations that are otherwise difficult
    to wire throughout the UI code.
    """

    visible_signature_desired: bool = False
    """
    Set to `True` if the user explicitly specifies `--field` with a bounding box
    or passes `--style-name` explicitly.
    """


@dataclass
class CLIContext:
    """
    Context object that cobbles together various CLI settings values that were
    gathered by various subcommands during the lifetime of a CLI invocation,
    either from configuration or from command line arguments.
    This object is passed around as a ``click`` context object.

    Not all settings are applicable to all subcommands, so all values are
    optional.
    """

    sig_settings: Optional[PdfSignatureMetadata] = None
    """
    The settings that will be used to produce a new signature.
    """

    config: Optional[CLIConfig] = None
    """
    Values for CLI configuration settings.
    """

    existing_fields_only: bool = False
    """
    Whether signing operations should use existing fields only.
    """

    timestamp_url: Optional[str] = None
    """
    Endpoint URL for the timestamping service to use.
    """

    stamp_style: Optional[BaseStampStyle] = None
    """
    Stamp style to use for generating visual signature appearances, if
    applicable.
    """

    stamp_url: Optional[str] = None
    """
    For QR stamp styles, defines the URL used to generate the QR code.
    """

    new_field_spec: Optional[SigFieldSpec] = None
    """
    Field spec used to generate new signature fields, if applicable.
    """

    prefer_pss: bool = False
    """
    When working with RSA keys, prefer RSASSA-PSS signing if available.
    """

    detach_pem: bool = False
    """
    When producing detached signature payloads (i.e. non-PDF CMS), save the
    result in a PEM file instead of in a DER file.
    """

    lenient: bool = False
    """
    Process PDF files in nonstrict mode.
    """

    ux: UXContext = field(default_factory=UXContext)
    """
    UX information.
    """
