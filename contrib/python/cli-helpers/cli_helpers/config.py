# -*- coding: utf-8 -*-
"""Read and write an application's config files."""

from __future__ import unicode_literals
import io
import logging
import os

from configobj import ConfigObj, ConfigObjError
from validate import ValidateError, Validator

from .compat import MAC, text_type, UserDict, WIN

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Base class for exceptions in this module."""

    pass


class DefaultConfigValidationError(ConfigError):
    """Indicates the default config file did not validate correctly."""

    pass


class Config(UserDict, object):
    """Config reader/writer class.

    :param str app_name: The application's name.
    :param str app_author: The application author/organization.
    :param str filename: The config filename to look for (e.g. ``config``).
    :param dict/str default: The default config values or absolute path to
                             config file.
    :param bool validate: Whether or not to validate the config file.
    :param bool write_default: Whether or not to write the default config
                               file to the user config directory if it doesn't
                               already exist.
    :param tuple additional_dirs: Additional directories to check for a config
                                  file.
    """

    def __init__(
        self,
        app_name,
        app_author,
        filename,
        default=None,
        validate=False,
        write_default=False,
        additional_dirs=(),
    ):
        super(Config, self).__init__()
        #: The :class:`ConfigObj` instance.
        self.data = ConfigObj(encoding="utf8")

        self.default = {}
        self.default_file = self.default_config = None
        self.config_filenames = []

        self.app_name, self.app_author = app_name, app_author
        self.filename = filename
        self.write_default = write_default
        self.validate = validate
        self.additional_dirs = additional_dirs

        if isinstance(default, dict):
            self.default = default
            self.update(default)
        elif isinstance(default, text_type):
            self.default_file = default
        elif default is not None:
            raise TypeError(
                '"default" must be a dict or {}, not {}'.format(
                    text_type.__name__, type(default)
                )
            )

        if self.write_default and not self.default_file:
            raise ValueError(
                'Cannot use "write_default" without specifying ' "a default file."
            )

        if self.validate and not self.default_file:
            raise ValueError(
                'Cannot use "validate" without specifying a ' "default file."
            )

    def read_default_config(self):
        """Read the default config file.

        :raises DefaultConfigValidationError: There was a validation error with
                                              the *default* file.
        """
        if self.validate:
            self.default_config = ConfigObj(
                configspec=self.default_file,
                list_values=False,
                _inspec=True,
                encoding="utf8",
            )
            # ConfigObj does not set the encoding on the configspec.
            self.default_config.configspec.encoding = "utf8"

            valid = self.default_config.validate(
                Validator(), copy=True, preserve_errors=True
            )
            if valid is not True:
                for name, section in valid.items():
                    if section is True:
                        continue
                    for key, value in section.items():
                        if isinstance(value, ValidateError):
                            raise DefaultConfigValidationError(
                                'section [{}], key "{}": {}'.format(name, key, value)
                            )
        elif self.default_file:
            self.default_config, _ = self.read_config_file(self.default_file)

        self.update(self.default_config)

    def read(self):
        """Read the default, additional, system, and user config files.

        :raises DefaultConfigValidationError: There was a validation error with
                                              the *default* file.
        """
        if self.default_file:
            self.read_default_config()
        return self.read_config_files(self.all_config_files())

    def user_config_file(self):
        """Get the absolute path to the user config file."""
        return os.path.join(
            get_user_config_dir(self.app_name, self.app_author), self.filename
        )

    def system_config_files(self):
        """Get a list of absolute paths to the system config files."""
        return [
            os.path.join(f, self.filename)
            for f in get_system_config_dirs(self.app_name, self.app_author)
        ]

    def additional_files(self):
        """Get a list of absolute paths to the additional config files."""
        return [os.path.join(f, self.filename) for f in self.additional_dirs]

    def all_config_files(self):
        """Get a list of absolute paths to all the config files."""
        return (
            self.additional_files()
            + self.system_config_files()
            + [self.user_config_file()]
        )

    def write_default_config(self, overwrite=False):
        """Write the default config to the user's config file.

        :param bool overwrite: Write over an existing config if it exists.
        """
        destination = self.user_config_file()
        if not overwrite and os.path.exists(destination):
            return

        with io.open(destination, mode="wb") as f:
            self.default_config.write(f)

    def write(self, outfile=None, section=None):
        """Write the current config to a file (defaults to user config).

        :param str outfile: The path to the file to write to.
        :param None/str section: The config section to write, or :data:`None`
                                 to write the entire config.
        """
        with io.open(outfile or self.user_config_file(), "wb") as f:
            self.data.write(outfile=f, section=section)

    def read_config_file(self, f):
        """Read a config file *f*.

        :param str f: The path to a file to read.
        """
        configspec = self.default_file if self.validate else None
        try:
            config = ConfigObj(
                infile=f, configspec=configspec, interpolation=False, encoding="utf8"
            )
            # ConfigObj does not set the encoding on the configspec.
            if config.configspec is not None:
                config.configspec.encoding = "utf8"
        except ConfigObjError as e:
            logger.warning(
                "Unable to parse line {} of config file {}".format(e.line_number, f)
            )
            config = e.config

        valid = True
        if self.validate:
            valid = config.validate(Validator(), preserve_errors=True, copy=True)
        if bool(config):
            self.config_filenames.append(config.filename)

        return config, valid

    def read_config_files(self, files):
        """Read a list of config files.

        :param iterable files: An iterable (e.g. list) of files to read.
        """
        errors = {}
        for _file in files:
            config, valid = self.read_config_file(_file)
            self.update(config)
            if valid is not True:
                errors[_file] = valid
        return errors or True


def get_user_config_dir(app_name, app_author, roaming=True, force_xdg=True):
    """Returns the config folder for the application.  The default behavior
    is to return whatever is most appropriate for the operating system.

    For an example application called ``"My App"`` by ``"Acme"``,
    something like the following folders could be returned:

    macOS (non-XDG):
      ``~/Library/Application Support/My App``
    Mac OS X (XDG):
      ``~/.config/my-app``
    Unix:
      ``~/.config/my-app``
    Windows 7 (roaming):
      ``C:\\Users\\<user>\\AppData\\Roaming\\Acme\\My App``
    Windows 7 (not roaming):
      ``C:\\Users\\<user>\\AppData\\Local\\Acme\\My App``

    :param app_name: the application name. This should be properly capitalized
                     and can contain whitespace.
    :param app_author: The app author's name (or company). This should be
                       properly capitalized and can contain whitespace.
    :param roaming: controls if the folder should be roaming or not on Windows.
                    Has no effect on non-Windows systems.
    :param force_xdg: if this is set to `True`, then on macOS the XDG Base
                      Directory Specification will be followed. Has no effect
                      on non-macOS systems.

    """
    if WIN:
        key = "APPDATA" if roaming else "LOCALAPPDATA"
        folder = os.path.expanduser(os.environ.get(key, "~"))
        return os.path.join(folder, app_author, app_name)
    if MAC and not force_xdg:
        return os.path.join(
            os.path.expanduser("~/Library/Application Support"), app_name
        )
    return os.path.join(
        os.path.expanduser(os.environ.get("XDG_CONFIG_HOME", "~/.config")),
        _pathify(app_name),
    )


def get_system_config_dirs(app_name, app_author, force_xdg=True):
    r"""Returns a list of system-wide config folders for the application.

    For an example application called ``"My App"`` by ``"Acme"``,
    something like the following folders could be returned:

    macOS (non-XDG):
      ``['/Library/Application Support/My App']``
    Mac OS X (XDG):
      ``['/etc/xdg/my-app']``
    Unix:
      ``['/etc/xdg/my-app']``
    Windows 7:
      ``['C:\ProgramData\Acme\My App']``

    :param app_name: the application name. This should be properly capitalized
                     and can contain whitespace.
    :param app_author: The app author's name (or company). This should be
                       properly capitalized and can contain whitespace.
    :param force_xdg: if this is set to `True`, then on macOS the XDG Base
                      Directory Specification will be followed. Has no effect
                      on non-macOS systems.

    """
    if WIN:
        folder = os.environ.get("PROGRAMDATA")
        return [os.path.join(folder, app_author, app_name)]
    if MAC and not force_xdg:
        return [os.path.join("/Library/Application Support", app_name)]
    dirs = os.environ.get("XDG_CONFIG_DIRS", "/etc/xdg")
    paths = [os.path.expanduser(x) for x in dirs.split(os.pathsep)]
    return [os.path.join(d, _pathify(app_name)) for d in paths]


def _pathify(s):
    """Convert spaces to hyphens and lowercase a string."""
    return "-".join(s.split()).lower()
