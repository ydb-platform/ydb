# Copyright (c) 2011-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TextIO, Sequence
import os
import sys
from pathlib import Path
from configparser import ConfigParser

# Recommended uses of the global object "options":
# import ezdxf
# value = ezdxf.options.<attribute>
#
# alternative:
# from ezdxf._options import options

TRUE_STATE = {"True", "true", "On", "on", "1"}
CORE = "core"
BROWSE_COMMAND = "browse-command"
VIEW_COMMAND = "view-command"
DRAW_COMMAND = "draw-command"
EZDXF_INI = "ezdxf.ini"
EZDXF = "ezdxf"
XDG_CONFIG_HOME = "XDG_CONFIG_HOME"
CONFIG_DIRECTORY = ".config"
ODAFC_ADDON = "odafc-addon"
OPENSCAD_ADDON = "openscad-addon"
DRAWING_ADDON = "drawing-addon"
DIR_SEPARATOR = "\n"


def xdg_path(xdg_var: str, directory: str) -> Path:
    xdg_home = os.environ.get(xdg_var)
    if xdg_home:
        # should default to $HOME/<directory> e.g. $HOME/.config
        home = Path(xdg_home).expanduser()
    else:
        # replicate structure
        home = Path("~").expanduser() / directory
    return home / EZDXF


def config_home_path() -> Path:
    return xdg_path(XDG_CONFIG_HOME, CONFIG_DIRECTORY)


def default_config_files() -> list[Path]:
    config_paths = [
        config_home_path() / EZDXF_INI,
        Path(f"./{EZDXF_INI}"),
    ]
    return config_paths


def default_config() -> ConfigParser:
    config = ConfigParser()
    config[CORE] = {
        "DEFAULT_DIMENSION_TEXT_STYLE": "OpenSansCondensed-Light",
        "TEST_FILES": "",
        "SUPPORT_DIRS": "",
        "LOAD_PROXY_GRAPHICS": "true",
        "STORE_PROXY_GRAPHICS": "true",
        "LOG_UNPROCESSED_TAGS": "false",
        "FILTER_INVALID_XDATA_GROUP_CODES": "true",
        "WRITE_FIXED_META_DATA_FOR_TESTING": "false",
        "DISABLE_C_EXT": "false",
    }
    config[BROWSE_COMMAND] = {
        "TEXT_EDITOR": r'"C:\Program Files\Notepad++\notepad++.exe" '
        r'"{filename}" -n{num}',
        "ICON_SIZE": "32",
    }
    config[ODAFC_ADDON] = {
        "WIN_EXEC_PATH": r'"C:\Program Files\ODA\ODAFileConverter\ODAFileConverter.exe"',
        "UNIX_EXEC_PATH": "",
    }
    config[OPENSCAD_ADDON] = {
        "WIN_EXEC_PATH": r'"C:\Program Files\OpenSCAD\openscad.exe"'
    }
    config[DRAWING_ADDON] = {
        # These options are just for testing scenarios!
        "TRY_PYSIDE6": "true",
        "TRY_PYQT5": "true",
        # Order for resolving SHX fonts: 1. "t"=TrueType; 2. "s"=SHX; 3. "l"=LFF
        "SHX_RESOLVE_ORDER": "tsl",
    }
    return config


def config_files() -> list[Path]:
    # Loading order for config files:
    # 1. user home directory:
    #    "$XDG_CONFIG_HOME/ezdxf/ezdxf.ini" or
    #    "~/.config/ezdxf/ezdxf.ini"
    # 2. current working directory "./ezdxf.ini"
    # 3. config file specified by EZDXF_CONFIG_FILE

    paths = default_config_files()
    env_cfg = os.getenv("EZDXF_CONFIG_FILE", "")
    if env_cfg:
        paths.append(Path(env_cfg))
    return paths


def load_config_files(paths: list[Path]) -> ConfigParser:
    config = default_config()
    try:
        config.read(paths, encoding="utf8")
    except UnicodeDecodeError as e:
        print(str(e))
        print(f"Paths: {paths}")
        print("Maybe a file with UTF16 LE-BOM encoding. (Powershell!!!)")
        exit(1)
    # environment variables override config files
    for name, env_name in [
        ("TEST_FILES", "EZDXF_TEST_FILES"),
        ("DISABLE_C_EXT", "EZDXF_DISABLE_C_EXT"),
    ]:
        value = os.environ.get(env_name, "")
        if value:
            config[CORE][name] = value
    return config


def boolstr(value: bool) -> str:
    return str(value).lower()


class Options:
    CORE = CORE
    BROWSE_COMMAND = BROWSE_COMMAND
    VIEW_COMMAND = VIEW_COMMAND
    DRAW_COMMAND = DRAW_COMMAND

    CONFIG_VARS = [
        "EZDXF_DISABLE_C_EXT",
        "EZDXF_TEST_FILES",
        "EZDXF_CONFIG_FILE",
    ]

    def __init__(self) -> None:
        paths = config_files()
        self._loaded_paths: list[Path] = [p for p in paths if p.exists()]
        self._config = load_config_files(paths)
        # needs fast access:
        self.log_unprocessed_tags = True
        # Activate/deactivate Matplotlib support (e.g. for testing)
        self._use_c_ext = False  # set ezdxf.acc.__init__!
        self.debug = False
        self.update_cached_options()

    def set(self, section: str, key: str, value: str) -> None:
        self._config.set(section, key, value)

    def get(self, section: str, key: str, default: str = "") -> str:
        return self._config.get(section, key, fallback=default)

    def get_bool(self, section: str, key: str, default: bool = False) -> bool:
        return self._config.getboolean(section, key, fallback=default)

    def get_int(self, section: str, key: str, default: int = 0) -> int:
        return self._config.getint(section, key, fallback=default)

    def get_float(self, section: str, key: str, default: float = 0.0) -> float:
        return self._config.getfloat(section, key, fallback=default)

    def update_cached_options(self) -> None:
        self.log_unprocessed_tags = self.get_bool(
            Options.CORE, "LOG_UNPROCESSED_TAGS", default=True
        )

    def rewrite_cached_options(self):
        # rewrite cached options
        self._config.set(
            Options.CORE,
            "LOG_UNPROCESSED_TAGS",
            boolstr(self.log_unprocessed_tags),
        )

    @property
    def loaded_config_files(self) -> tuple[Path, ...]:
        return tuple(self._loaded_paths)

    def read_file(self, filename: str) -> None:
        """Append content from config file `filename`, but does not reset the
        configuration.
        """
        try:
            self._config.read(filename)
        except IOError as e:
            print(str(e))
        else:
            self._loaded_paths.append(Path(filename))
            self.update_cached_options()

    def write(self, fp: TextIO) -> None:
        """Write current configuration into given file object, the file object
        must be a writeable text file with 'utf8' encoding.
        """
        self.rewrite_cached_options()
        try:
            self._config.write(fp)
        except IOError as e:
            print(str(e))

    def write_file(self, filename: str = EZDXF_INI) -> None:
        """Write current configuration into file `filename`."""
        with open(os.path.expanduser(filename), "wt", encoding="utf8") as fp:
            self.write(fp)

    @property
    def filter_invalid_xdata_group_codes(self) -> bool:
        return self.get_bool(CORE, "FILTER_INVALID_XDATA_GROUP_CODES", default=True)

    @property
    def default_dimension_text_style(self) -> str:
        return self.get(
            CORE,
            "DEFAULT_DIMENSION_TEXT_STYLE",
            default="OpenSansCondensed-Light",
        )

    @default_dimension_text_style.setter
    def default_dimension_text_style(self, style: str) -> None:
        self.set(
            CORE,
            "DEFAULT_DIMENSION_TEXT_STYLE",
            style,
        )

    @property
    def support_dirs(self) -> list[str]:
        return [d for d in self.get(CORE, "SUPPORT_DIRS", "").split(DIR_SEPARATOR) if d]

    @support_dirs.setter
    def support_dirs(self, support_dirs: Sequence[str]) -> None:
        self.set(CORE, "SUPPORT_DIRS", DIR_SEPARATOR.join(support_dirs))

    @property
    def test_files(self) -> str:
        return os.path.expanduser(self.get(CORE, "TEST_FILES"))

    @property
    def test_files_path(self) -> Path:
        return Path(self.test_files)

    @property
    def load_proxy_graphics(self) -> bool:
        return self.get_bool(CORE, "LOAD_PROXY_GRAPHICS", default=True)

    @load_proxy_graphics.setter
    def load_proxy_graphics(self, value: bool) -> None:
        self.set(CORE, "LOAD_PROXY_GRAPHICS", boolstr(value))

    @property
    def store_proxy_graphics(self) -> bool:
        return self.get_bool(CORE, "STORE_PROXY_GRAPHICS", default=True)

    @store_proxy_graphics.setter
    def store_proxy_graphics(self, value: bool) -> None:
        self.set(CORE, "STORE_PROXY_GRAPHICS", boolstr(value))

    @property
    def write_fixed_meta_data_for_testing(self) -> bool:
        # Enable this option to always create same meta data for testing
        # scenarios, e.g. to use a diff like tool to compare DXF documents.
        return self.get_bool(CORE, "WRITE_FIXED_META_DATA_FOR_TESTING", default=False)

    @write_fixed_meta_data_for_testing.setter
    def write_fixed_meta_data_for_testing(self, state: bool) -> None:
        self.set(CORE, "write_fixed_meta_data_for_testing", boolstr(state))

    @property
    def disable_c_ext(self) -> bool:
        """Disable C-extensions if ``True``."""
        return self.get_bool(CORE, "DISABLE_C_EXT", default=False)

    @property
    def use_c_ext(self) -> bool:
        """Returns ``True`` if the C-extensions are in use."""
        return self._use_c_ext

    def preserve_proxy_graphics(self, state: bool = True) -> None:
        """Enable/disable proxy graphic load/store support."""
        value = boolstr(state)
        self.set(CORE, "LOAD_PROXY_GRAPHICS", value)
        self.set(CORE, "STORE_PROXY_GRAPHICS", value)

    def print(self):
        """Print current configuration to `stdout`."""
        self._config.write(sys.stdout)

    def write_home_config(self):
        """Write current configuration into file "~/.config/ezdxf/ezdxf.ini" or
        "XDG_CONFIG_HOME/ezdxf/ezdxf.ini".
        """

        home_path = config_home_path()
        if not home_path.exists():
            try:
                home_path.mkdir(parents=True)
            except IOError as e:
                print(str(e))
                return

        filename = str(home_path / EZDXF_INI)
        try:
            self.write_file(filename)
        except IOError as e:
            print(str(e))
        else:
            print(f"created config file: '{filename}'")

    def reset(self):
        self._loaded_paths = []
        self._config = default_config()
        self.update_cached_options()

    @staticmethod
    def delete_default_config_files():
        for file in default_config_files():
            if file.exists():
                try:
                    file.unlink()
                    print(f"deleted config file: '{file}'")
                except IOError as e:
                    print(str(e))

    @staticmethod
    def xdg_path(xdg_var: str, directory: str) -> Path:
        return xdg_path(xdg_var, directory)


# Global Options
options = Options()
