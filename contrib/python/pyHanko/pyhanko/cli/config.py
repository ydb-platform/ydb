from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional, Type, Union

import yaml
from pyhanko_certvalidator import ValidationContext

from pyhanko.config.errors import ConfigurationError
from pyhanko.config.logging import LogConfig, parse_logging_config
from pyhanko.config.trust import DEFAULT_TIME_TOLERANCE, parse_trust_config
from pyhanko.sign.signers import DEFAULT_SIGNING_STAMP_STYLE
from pyhanko.sign.validation.settings import KeyUsageConstraints
from pyhanko.stamp import BaseStampStyle, QRStampStyle, TextStampStyle


@dataclass
class CLIConfig:
    """
    CLI configuration settings.
    """

    validation_contexts: Dict[str, dict]
    """
    Named validation contexts. The values in this dictionary
    are themselves dictionaries that support the following keys:

     * ``trust``: path to a root certificate or list of such paths
     * ``trust-replace``: whether the value of the ``trust`` setting should
       replace the system trust, or add to it
     * ``other-certs``: paths to other relevant certificates that are not
       trusted by fiat.
     * ``time-tolerance``: a time drift tolerance setting in seconds
     * ``retroactive-revinfo``: whether to consider revocation information
       retroactively valid
     * ``signer-key-usage-policy``: Signer key usage requirements. See
       :class:`.KeyUsageConstraints`.

    There are two settings that are deprecated but still supported for backwards
    compatibility:

     * ``signer-key-usage``: Supplanted by ``signer-key-usage-policy``
     * ``signer-extd-key-usage``: Supplanted by ``signer-key-usage-policy``

    These may eventually be removed.

    Callers should not process this information directly, but rely on
    :meth:`get_validation_context` instead.
    """

    stamp_styles: Dict[str, dict]
    """
    Named stamp styles. The type of style is selected by the ``type`` key, which
    can be either ``qr`` or ``text`` (the default is ``text``).
    For other settings values, see :class:.`QRStampStyle` and
    :class:`.TextStampStyle`.


    Callers should not process this information directly, but rely on
    :meth:`get_stamp_style` instead.
    """

    default_validation_context: str
    """
    The name of the default validation context.
    The default value for this setting is ``default``.
    """

    default_stamp_style: str
    """
    The name of the default stamp style.
    The default value for this setting is ``default``.
    """

    time_tolerance: timedelta
    """
    Time drift tolerance (global default).
    """

    retroactive_revinfo: bool
    """
    Whether to consider revocation information retroactively valid
    (global default).
    """

    raw_config: dict
    """
    The raw config data parsed into a Python dictionary.
    """

    # TODO graceful error handling for syntax & type issues?

    def _get_validation_settings_raw(self, name=None):
        name = name or self.default_validation_context
        try:
            return self.validation_contexts[name]
        except KeyError:
            raise ConfigurationError(
                f"There is no validation context named '{name}'."
            )

    def get_validation_context(
        self, name: Optional[str] = None, as_dict: bool = False
    ):
        """
        Retrieve a validation context by name.

        :param name:
            The name of the validation context. If not supplied, the value
            of :attr:`default_validation_context` will be used.
        :param as_dict:
            If ``True`` return the settings as a keyword argument dictionary.
            If ``False`` (the default), return a
            :class:`~pyhanko_certvalidator.context.ValidationContext`
            object.
        """
        vc_config = self._get_validation_settings_raw(name)
        vc_kwargs = parse_trust_config(
            vc_config, self.time_tolerance, self.retroactive_revinfo
        )
        return vc_kwargs if as_dict else ValidationContext(**vc_kwargs)

    def get_signer_key_usages(
        self, name: Optional[str] = None
    ) -> KeyUsageConstraints:
        """
        Get a set of key usage constraints for a given validation context.

        :param name:
            The name of the validation context. If not supplied, the value
            of :attr:`default_validation_context` will be used.
        :return:
            A :class:`.KeyUsageConstraints` object.
        """
        vc_config = self._get_validation_settings_raw(name)

        try:
            policy_settings = dict(vc_config['signer-key-usage-policy'])
        except KeyError:
            policy_settings = {}

        # fallbacks to stay compatible with the simpler 0.5.0 signer-key-usage
        # and signer-extd-key-usage settings: copy old settings keys to
        # their corresponding values in the new one

        try:
            key_usage_strings = vc_config['signer-key-usage']
            policy_settings.setdefault('key-usage', key_usage_strings)
        except KeyError:
            pass

        try:
            key_usage_strings = vc_config['signer-extd-key-usage']
            policy_settings.setdefault('extd-key-usage', key_usage_strings)
        except KeyError:
            pass

        return KeyUsageConstraints.from_config(policy_settings)

    def get_stamp_style(
        self, name: Optional[str] = None
    ) -> Union[TextStampStyle, QRStampStyle]:
        """
        Retrieve a stamp style by name.

        :param name:
            The name of the style. If not supplied, the value
            of :attr:`default_stamp_style` will be used.
        :return:
            A :class:`TextStampStyle` or `QRStampStyle` object.
        """
        name = name or self.default_stamp_style
        config = self.stamp_styles.get(name, None)
        if config is None:
            raise ConfigurationError(f"There is no stamp style named '{name}'.")
        try:
            style_config = dict(config)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(
                f"Could not process stamp style named '{name}'"
            ) from e
        cls = STAMP_STYLE_TYPES[style_config.pop('type', 'text')]
        return cls.from_config(style_config)


@dataclass(frozen=True)
class CLIRootConfig:
    """
    Config settings that are only relevant tothe CLI root and are not exposed
    to subcommands and plugins.
    """

    config: CLIConfig
    """
    General CLI config.
    """

    log_config: Dict[Optional[str], LogConfig]
    """
    Per-module logging configuration. The keys in this dictionary are
    module names, the :class:`.LogConfig` values define the logging settings.

    The ``None`` key houses the configuration for the root logger, if any.
    """

    plugin_endpoints: List[str]
    """
    List of plugin endpoints to load, of the form
    ``package.module:PluginClass``.
    See :class:`~pyhanko.cli.plugin_api.SigningCommandPlugin`.

    The value of this setting is ignored if ``--no-plugins`` is passed.

    .. note::
        This is convenient for importing plugin classes that don't live in
        installed packages for some reason or another.

        Plugins that are part of packages should define their endpoints
        in the package metadata, which will allow them to be discovered
        automatically. See the docs for
        :class:`~pyhanko.cli.plugin_api.SigningCommandPlugin` for more
        information.
    """


# TODO allow CRL/OCSP loading here as well (esp. CRL loading might be useful
#  in some cases)
# Time-related settings are probably better off in the CLI.


DEFAULT_VALIDATION_CONTEXT = DEFAULT_STAMP_STYLE = 'default'
STAMP_STYLE_TYPES: Dict[str, Type[BaseStampStyle]] = {
    'qr': QRStampStyle,
    'text': TextStampStyle,
}


def parse_cli_config(yaml_str) -> CLIRootConfig:
    config_dict = yaml.safe_load(yaml_str) or {}
    return CLIRootConfig(
        **process_root_config_settings(config_dict),
        config=CLIConfig(
            **process_config_dict(config_dict), raw_config=config_dict
        ),
    )


def process_root_config_settings(config_dict: dict) -> dict:
    plugins = config_dict.get('plugins', [])
    # logging config
    log_config_spec = config_dict.get('logging', {})
    log_config = parse_logging_config(log_config_spec)
    return dict(
        log_config=log_config,
        plugin_endpoints=plugins,
    )


def process_config_dict(config_dict: dict) -> dict:
    # validation context config
    vcs: Dict[str, dict] = {DEFAULT_VALIDATION_CONTEXT: {}}
    try:
        vc_specs = config_dict['validation-contexts']
        vcs.update(vc_specs)
    except KeyError:
        pass

    # stamp style config
    # TODO this style is obviously not suited for non-signing scenarios
    #  (but it'll do for now)
    stamp_configs = {
        DEFAULT_STAMP_STYLE: {
            'stamp-text': DEFAULT_SIGNING_STAMP_STYLE.stamp_text,
            'background': '__stamp__',
        }
    }
    try:
        stamp_specs = config_dict['stamp-styles']
        stamp_configs.update(stamp_specs)
    except KeyError:
        pass

    # some misc settings
    default_vc = config_dict.get(
        'default-validation-context', DEFAULT_VALIDATION_CONTEXT
    )
    default_stamp_style = config_dict.get(
        'default-stamp-style', DEFAULT_STAMP_STYLE
    )
    time_tolerance_seconds = config_dict.get(
        'time-tolerance', DEFAULT_TIME_TOLERANCE.seconds
    )
    if not isinstance(time_tolerance_seconds, int):
        raise ConfigurationError(
            "time-tolerance parameter must be specified in seconds"
        )

    time_tolerance = timedelta(seconds=time_tolerance_seconds)
    retroactive_revinfo = bool(config_dict.get('retroactive-revinfo', False))
    return dict(
        validation_contexts=vcs,
        default_validation_context=default_vc,
        time_tolerance=time_tolerance,
        retroactive_revinfo=retroactive_revinfo,
        stamp_styles=stamp_configs,
        default_stamp_style=default_stamp_style,
    )
