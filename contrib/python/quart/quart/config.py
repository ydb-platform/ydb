from __future__ import annotations

import importlib
import importlib.util
import json
import os
from configparser import ConfigParser
from datetime import timedelta
from typing import Any, Callable, Dict, IO, Mapping, Optional, Union

from .typing import FilePath
from .utils import file_path_to_path

DEFAULT_CONFIG = {
    "APPLICATION_ROOT": None,
    "BODY_TIMEOUT": 60,  # Second
    "DEBUG": None,
    "ENV": None,
    "JSON_AS_ASCII": True,
    "JSON_SORT_KEYS": True,
    "JSONIFY_MIMETYPE": "application/json",
    "JSONIFY_PRETTYPRINT_REGULAR": False,
    "MAX_CONTENT_LENGTH": 16 * 1024 * 1024,  # 16 MB Limit
    "MAX_COOKIE_SIZE": 4093,
    "PERMANENT_SESSION_LIFETIME": timedelta(days=31),
    "PREFER_SECURE_URLS": False,  # Replaces PREFERRED_URL_SCHEME to allow for WebSocket scheme
    "PRESERVE_CONTEXT_ON_EXCEPTION": None,
    "PROPAGATE_EXCEPTIONS": None,
    "RESPONSE_TIMEOUT": 60,  # Second
    "SECRET_KEY": None,
    "SEND_FILE_MAX_AGE_DEFAULT": timedelta(hours=12),
    "SERVER_NAME": None,
    "SESSION_COOKIE_DOMAIN": None,
    "SESSION_COOKIE_HTTPONLY": True,
    "SESSION_COOKIE_NAME": "session",
    "SESSION_COOKIE_PATH": None,
    "SESSION_COOKIE_SAMESITE": None,
    "SESSION_COOKIE_SECURE": False,
    "SESSION_REFRESH_EACH_REQUEST": True,
    "TEMPLATES_AUTO_RELOAD": None,
    "TESTING": False,
    "TRAP_HTTP_EXCEPTIONS": False,
}


class ConfigAttribute:
    """Implements a property descriptor for objects with a config attribute.

    When used as a class instance it will look up the key on the class
    config object, for example:

    .. code-block:: python

        class Object:
            config = {}
            foo = ConfigAttribute('foo')

        obj = Object()
        obj.foo = 'bob'
        assert obj.foo == obj.config['foo']
    """

    def __init__(self, key: str, converter: Optional[Callable] = None) -> None:
        self.key = key
        self.converter = converter

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self
        result = instance.config[self.key]
        if self.converter is not None:
            return self.converter(result)
        else:
            return result

    def __set__(self, instance: Any, value: Any) -> None:
        instance.config[self.key] = value


class Config(dict):
    """Extends a standard Python dictionary with additional load (from) methods.

    Note that the convention (as enforced when loading) is that
    configuration keys are upper case. Whilst you can set lower case
    keys it is not recommended.
    """

    def __init__(self, root_path: FilePath, defaults: Optional[dict] = None) -> None:
        super().__init__(defaults or {})
        self.root_path = file_path_to_path(root_path)

    def from_envvar(self, variable_name: str, silent: bool = False) -> bool:
        """Load the configuration from a location specified in the environment.

        This will load a cfg file using :meth:`from_pyfile` from the
        location specified in the environment, for example the two blocks
        below are equivalent.

        .. code-block:: python

            app.config.from_envvar('CONFIG')

        .. code-block:: python

            filename = os.environ['CONFIG']
            app.config.from_pyfile(filename)
        """
        value = os.environ.get(variable_name)
        if value is None and not silent:
            raise RuntimeError(
                f"Environment variable {variable_name} is not present, cannot load config"
            )
        return self.from_pyfile(value)

    def from_prefixed_env(
        self, prefix: str = "QUART", *, loads: Callable[[str], Any] = json.loads
    ) -> bool:
        """Load any environment variables that start with the prefix.

        The prefix (default ``QUART_``) is dropped from the env key
        for the config key. Values are passed through a loading
        function to attempt to convert them to more specific types
        than strings.

        Keys are loaded in :func:`sorted` order.

        The default loading function attempts to parse values as any
        valid JSON type, including dicts and lists.  Specific items in
        nested dicts can be set by separating the keys with double
        underscores (``__``). If an intermediate key doesn't exist, it
        will be initialized to an empty dict.

        Arguments:
            prefix: Load env vars that start with this prefix,
                separated with an underscore (``_``).
            loads: Pass each string value to this function and use the
                returned value as the config value. If any error is
                raised it is ignored and the value remains a
                string. The default is :func:`json.loads`.
        """
        prefix = f"{prefix}_"
        len_prefix = len(prefix)

        for key in sorted(os.environ):
            if not key.startswith(prefix):
                continue

            value = os.environ[key]

            try:
                value = loads(value)
            except Exception:
                # Keep the value as a string if loading failed.
                pass

            # Change to key.removeprefix(prefix) on Python >= 3.9.
            key = key[len_prefix:]

            if "__" not in key:
                # A non-nested key, set directly.
                self[key] = value
                continue

            # Traverse nested dictionaries with keys separated by "__".
            current = self
            *parts, tail = key.split("__")

            for part in parts:
                # If an intermediate dict does not exist, create it.
                if part not in current:
                    current[part] = {}

                current = current[part]

            current[tail] = value

        return True

    def from_pyfile(self, filename: str, silent: bool = False) -> bool:
        """Load the configuration from a Python cfg or py file.

        See Python's ConfigParser docs for details on the cfg format.
        It is a common practice to load the defaults from the source
        using the :meth:`from_object` and then override with a cfg or
        py file, for example

        .. code-block:: python

            app.config.from_object('config_module')
            app.config.from_pyfile('production.cfg')

        Arguments:
            filename: The filename which when appended to
                :attr:`root_path` gives the path to the file

        """
        file_path = self.root_path / filename
        try:
            spec = importlib.util.spec_from_file_location("module.name", file_path)
            if spec is None:  # Likely passed a cfg file
                parser = ConfigParser()
                parser.optionxform = str  # type: ignore # Prevents lowercasing of keys
                with open(file_path) as file_:
                    config_str = "[section]\n" + file_.read()
                parser.read_string(config_str)
                self.from_mapping(parser["section"])
            else:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.from_object(module)
        except (FileNotFoundError, IsADirectoryError):
            if not silent:
                raise
        return True

    def from_object(self, instance: Union[object, str]) -> None:
        """Load the configuration from a Python object.

        This can be used to reference modules or objects within
        modules for example,

        .. code-block:: python

            app.config.from_object('module')
            app.config.from_object('module.instance')
            from module import instance
            app.config.from_object(instance)

        are valid.

        Arguments:
            instance: Either a str referencing a python object or the
                object itself.

        """
        if isinstance(instance, str):
            try:
                path, config = instance.rsplit(".", 1)
            except ValueError:
                path = instance
                instance = importlib.import_module(path)
            else:
                module = importlib.import_module(path)
                instance = getattr(module, config)

        for key in dir(instance):
            if key.isupper():
                self[key] = getattr(instance, key)

    def from_json(self, filename: str, silent: bool = False) -> None:
        """Load the configuration values from a JSON formatted file.

        This allows configuration to be loaded as so

        .. code-block:: python

            app.config.from_json('config.json')

        Arguments:
            filename: The filename which when appended to
                :attr:`root_path` gives the path to the file.
            silent: If True any errors will fail silently.
        """
        self.from_file(filename, json.load, silent=silent)

    def from_file(
        self, filename: str, load: Callable[[IO[Any]], Mapping], silent: bool = False
    ) -> bool:
        """Load the configuration from a data file.

        This allows configuration to be loaded as so

        .. code-block:: python

            app.config.from_file('config.toml', toml.load)
            app.config.from_file('config.json', json.load)

        Arguments:
            filename: The filename which when appended to
                :attr:`root_path` gives the path to the file.
            load: Callable that takes a file descriptor and
                returns a mapping loaded from the file.
            silent: If True any errors will fail silently.
        """
        file_path = self.root_path / filename
        try:
            with open(file_path) as file_:
                data = load(file_)
        except (FileNotFoundError, IsADirectoryError):
            if not silent:
                raise
            else:
                return False
        else:
            return self.from_mapping(data)

    def from_mapping(self, mapping: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> bool:
        """Load the configuration values from a mapping.

        This allows either a mapping to be directly passed or as
        keyword arguments, for example,

        .. code-block:: python

            config = {'FOO': 'bar'}
            app.config.from_mapping(config)
            app.config.form_mapping(FOO='bar')

        Arguments:
            mapping: Optionally a mapping object.
            kwargs: Optionally a collection of keyword arguments to
                form a mapping.
        """
        mappings: Dict[str, Any] = {}
        if mapping is not None:
            mappings.update(mapping)
        mappings.update(kwargs)
        for key, value in mappings.items():
            if key.isupper():
                self[key] = value
        return True

    def get_namespace(
        self, namespace: str, lowercase: bool = True, trim_namespace: bool = True
    ) -> Dict[str, Any]:
        """Return a dictionary of keys within a namespace.

        A namespace is considered to be a key prefix, for example the
        keys ``FOO_A, FOO_BAR, FOO_B`` are all within the ``FOO``
        namespace. This method would return a dictionary with these
        keys and values present.

        .. code-block:: python

            config = {'FOO_A': 'a', 'FOO_BAR': 'bar', 'BAR': False}
            app.config.from_mapping(config)
            assert app.config.get_namespace('FOO_') == {'a': 'a', 'bar': 'bar'}

        Arguments:
            namespace: The namespace itself (should be uppercase).
            lowercase: Lowercase the keys in the returned dictionary.
            trim_namespace: Remove the namespace from the returned
                keys.
        """
        config = {}
        for key, value in self.items():
            if key.startswith(namespace):
                if trim_namespace:
                    new_key = key[len(namespace) :]
                else:
                    new_key = key
                if lowercase:
                    new_key = new_key.lower()
                config[new_key] = value
        return config

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {dict.__repr__(self)}>"
