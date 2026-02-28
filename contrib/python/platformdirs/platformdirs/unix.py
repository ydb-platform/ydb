"""Unix."""

from __future__ import annotations

import os
import sys
from configparser import ConfigParser
from pathlib import Path
from tempfile import gettempdir
from typing import TYPE_CHECKING, NoReturn

from ._xdg import XDGMixin
from .api import PlatformDirsABC

if TYPE_CHECKING:
    from collections.abc import Iterator

if sys.platform == "win32":

    def getuid() -> NoReturn:
        msg = "should only be used on Unix"
        raise RuntimeError(msg)

else:
    from os import getuid


class _UnixDefaults(PlatformDirsABC):
    """
    Default directories for Unix/Linux without XDG environment variable overrides.

    The XDG env var handling is in :class:`~platformdirs._xdg.XDGMixin`.
    """

    @property
    def user_data_dir(self) -> str:
        """
        :return: data directory tied to the user, e.g. ``~/.local/share/$appname/$version`` or
         ``$XDG_DATA_HOME/$appname/$version``
        """
        return self._append_app_name_and_version(os.path.expanduser("~/.local/share"))  # noqa: PTH111

    @property
    def _site_data_dirs(self) -> list[str]:
        return [self._append_app_name_and_version("/usr/local/share"), self._append_app_name_and_version("/usr/share")]

    @property
    def user_config_dir(self) -> str:
        """
        :return: config directory tied to the user, e.g. ``~/.config/$appname/$version`` or
         ``$XDG_CONFIG_HOME/$appname/$version``
        """
        return self._append_app_name_and_version(os.path.expanduser("~/.config"))  # noqa: PTH111

    @property
    def _site_config_dirs(self) -> list[str]:
        return [self._append_app_name_and_version("/etc/xdg")]

    @property
    def user_cache_dir(self) -> str:
        """
        :return: cache directory tied to the user, e.g. ``~/.cache/$appname/$version`` or
         ``$XDG_CACHE_HOME/$appname/$version``
        """
        return self._append_app_name_and_version(os.path.expanduser("~/.cache"))  # noqa: PTH111

    @property
    def site_cache_dir(self) -> str:
        """:return: cache directory shared by users, e.g. ``/var/cache/$appname/$version``"""
        return self._append_app_name_and_version("/var/cache")

    @property
    def user_state_dir(self) -> str:
        """
        :return: state directory tied to the user, e.g. ``~/.local/state/$appname/$version`` or
         ``$XDG_STATE_HOME/$appname/$version``
        """
        return self._append_app_name_and_version(os.path.expanduser("~/.local/state"))  # noqa: PTH111

    @property
    def user_log_dir(self) -> str:
        """:return: log directory tied to the user, same as `user_state_dir` if not opinionated else ``log`` in it"""
        path = self.user_state_dir
        if self.opinion:
            path = os.path.join(path, "log")  # noqa: PTH118
            self._optionally_create_directory(path)
        return path

    @property
    def user_documents_dir(self) -> str:
        """:return: documents directory tied to the user, e.g. ``~/Documents``"""
        return _get_user_media_dir("XDG_DOCUMENTS_DIR", "~/Documents")

    @property
    def user_downloads_dir(self) -> str:
        """:return: downloads directory tied to the user, e.g. ``~/Downloads``"""
        return _get_user_media_dir("XDG_DOWNLOAD_DIR", "~/Downloads")

    @property
    def user_pictures_dir(self) -> str:
        """:return: pictures directory tied to the user, e.g. ``~/Pictures``"""
        return _get_user_media_dir("XDG_PICTURES_DIR", "~/Pictures")

    @property
    def user_videos_dir(self) -> str:
        """:return: videos directory tied to the user, e.g. ``~/Videos``"""
        return _get_user_media_dir("XDG_VIDEOS_DIR", "~/Videos")

    @property
    def user_music_dir(self) -> str:
        """:return: music directory tied to the user, e.g. ``~/Music``"""
        return _get_user_media_dir("XDG_MUSIC_DIR", "~/Music")

    @property
    def user_desktop_dir(self) -> str:
        """:return: desktop directory tied to the user, e.g. ``~/Desktop``"""
        return _get_user_media_dir("XDG_DESKTOP_DIR", "~/Desktop")

    @property
    def user_runtime_dir(self) -> str:
        """
        :return: runtime directory tied to the user, e.g. ``$XDG_RUNTIME_DIR/$appname/$version``.

        If ``$XDG_RUNTIME_DIR`` is unset, tries the platform default (``/var/run/user/$(id -u)`` on
        FreeBSD/OpenBSD/NetBSD, ``/run/user/$(id -u)`` otherwise). If the default is not writable,
        falls back to a temporary directory.
        """
        if sys.platform.startswith(("freebsd", "openbsd", "netbsd")):
            path = f"/var/run/user/{getuid()}"
        else:
            path = f"/run/user/{getuid()}"
        if not os.access(path, os.W_OK):
            path = f"{gettempdir()}/runtime-{getuid()}"
        return self._append_app_name_and_version(path)

    @property
    def site_runtime_dir(self) -> str:
        """
        :return: runtime directory shared by users, e.g. ``/run/$appname/$version`` or \
        ``$XDG_RUNTIME_DIR/$appname/$version``.

        Note that this behaves almost exactly like `user_runtime_dir` if ``$XDG_RUNTIME_DIR`` is set, but will
        fall back to paths associated to the root user instead of a regular logged-in user if it's not set.

        If you wish to ensure that a logged-in root user path is returned e.g. ``/run/user/0``, use `user_runtime_dir`
        instead.

        For FreeBSD/OpenBSD/NetBSD, it would return ``/var/run/$appname/$version`` if ``$XDG_RUNTIME_DIR`` is not set.
        """
        if sys.platform.startswith(("freebsd", "openbsd", "netbsd")):
            path = "/var/run"
        else:
            path = "/run"
        return self._append_app_name_and_version(path)

    @property
    def site_data_path(self) -> Path:
        """:return: data path shared by users. Only return the first item, even if ``multipath`` is set to ``True``"""
        return self._first_item_as_path_if_multipath(self.site_data_dir)

    @property
    def site_config_path(self) -> Path:
        """:return: config path shared by users, returns the first item, even if ``multipath`` is set to ``True``"""
        return self._first_item_as_path_if_multipath(self.site_config_dir)

    @property
    def site_cache_path(self) -> Path:
        """:return: cache path shared by users. Only return the first item, even if ``multipath`` is set to ``True``"""
        return self._first_item_as_path_if_multipath(self.site_cache_dir)

    def iter_config_dirs(self) -> Iterator[str]:
        """:yield: all user and site configuration directories."""
        yield self.user_config_dir
        yield from self._site_config_dirs

    def iter_data_dirs(self) -> Iterator[str]:
        """:yield: all user and site data directories."""
        yield self.user_data_dir
        yield from self._site_data_dirs


class Unix(XDGMixin, _UnixDefaults):
    """
    On Unix/Linux, we follow the `XDG Basedir Spec <https://specifications.freedesktop.org/basedir/latest/>`_.

    The spec allows overriding directories with environment variables. The examples shown are the default values,
    alongside the name of the environment variable that overrides them. Makes use of the `appname
    <platformdirs.api.PlatformDirsABC.appname>`, `version <platformdirs.api.PlatformDirsABC.version>`, `multipath
    <platformdirs.api.PlatformDirsABC.multipath>`, `opinion <platformdirs.api.PlatformDirsABC.opinion>`, `ensure_exists
    <platformdirs.api.PlatformDirsABC.ensure_exists>`.
    """


def _get_user_media_dir(env_var: str, fallback_tilde_path: str) -> str:
    if media_dir := _get_user_dirs_folder(env_var):
        return media_dir
    return os.path.expanduser(fallback_tilde_path)  # noqa: PTH111


def _get_user_dirs_folder(key: str) -> str | None:
    """
    Return directory from user-dirs.dirs config file.

    See https://freedesktop.org/wiki/Software/xdg-user-dirs/.

    """
    user_dirs_config_path = Path(os.path.expanduser("~/.config")) / "user-dirs.dirs"  # noqa: PTH111
    if user_dirs_config_path.exists():
        parser = ConfigParser()

        with user_dirs_config_path.open() as stream:
            parser.read_string(f"[top]\n{stream.read()}")

        if key not in parser["top"]:
            return None

        path = parser["top"][key].strip('"')
        return path.replace("$HOME", os.path.expanduser("~"))  # noqa: PTH111

    return None


__all__ = [
    "Unix",
]
