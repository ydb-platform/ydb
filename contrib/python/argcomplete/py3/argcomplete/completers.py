# Copyright 2012-2023, Andrey Kislyuk and argcomplete contributors.
# Licensed under the Apache License. See https://github.com/kislyuk/argcomplete for more info.

import argparse
import os
import subprocess
from collections.abc import Callable, Generator, Iterable, Mapping
from shlex import quote
from typing import Final

_Ignored = object


def _call(*args, **kwargs):
    # TODO: replace "universal_newlines" with "text" once 3.6 support is dropped
    kwargs["universal_newlines"] = True
    try:
        return subprocess.check_output(*args, **kwargs).splitlines()
    except subprocess.CalledProcessError:
        return []


class BaseCompleter:
    """
    This is the base class that all argcomplete completers should subclass.
    """

    def __call__(
        self, *, prefix: str, action: argparse.Action, parser: argparse.ArgumentParser, parsed_args: argparse.Namespace
    ) -> Iterable[str]:
        raise NotImplementedError("This method should be implemented by a subclass.")


class ChoicesCompleter(BaseCompleter):
    choices: Final[Mapping[str, str | bytes]]

    def __init__(self, choices: Mapping[str, str | bytes]) -> None:
        self.choices = choices

    def _convert(self, choice):
        if not isinstance(choice, str):
            choice = str(choice)
        return choice

    def __call__(self, **kwargs: _Ignored) -> Iterable[str]:
        return (self._convert(c) for c in self.choices)


EnvironCompleter: Final[ChoicesCompleter] = ChoicesCompleter(os.environ)


class FilesCompleter(BaseCompleter):
    """
    File completer class, optionally takes a list of allowed extensions
    """

    allowednames: Final[list[str]]
    directories: Final[bool]

    def __init__(self, allowednames: Iterable[str] | str = (), directories: bool = True) -> None:
        # Fix if someone passes in a string instead of a list
        if isinstance(allowednames, (str, bytes)):
            allowednames = [allowednames]

        self.allowednames = [x.lstrip("*").lstrip(".") for x in allowednames]
        self.directories = directories

    def __call__(self, prefix: str, **kwargs: _Ignored) -> list[str]:
        completion: list[str] = []
        if self.allowednames:
            if self.directories:
                # Using 'bind' in this and the following commands is a workaround to a bug in bash
                # that was fixed in bash 5.3 but affects older versions. Environment variables are not treated
                # correctly in older versions and calling bind makes them available. For details, see
                # https://savannah.gnu.org/support/index.php?111125
                files = _call(
                    ["bash", "-c", "bind; compgen -A directory -- {p}".format(p=quote(prefix))],
                    stderr=subprocess.DEVNULL,
                )
                completion += [f + "/" for f in files]
            for x in self.allowednames:
                completion += _call(
                    ["bash", "-c", "bind; compgen -A file -X '!*.{0}' -- {p}".format(x, p=quote(prefix))],
                    stderr=subprocess.DEVNULL,
                )
        else:
            completion += _call(
                ["bash", "-c", "bind; compgen -A file -- {p}".format(p=quote(prefix))], stderr=subprocess.DEVNULL
            )
            anticomp = _call(
                ["bash", "-c", "bind; compgen -A directory -- {p}".format(p=quote(prefix))],
                stderr=subprocess.DEVNULL,
            )
            completion = list(set(completion) - set(anticomp))

            if self.directories:
                completion += [f + "/" for f in anticomp]
        return completion


class _FilteredFilesCompleter(BaseCompleter):
    predicate: Final[Callable[[str], bool]]

    def __init__(self, predicate: Callable[[str], bool]) -> None:
        """
        Create the completer

        A predicate accepts as its only argument a candidate path and either
        accepts it or rejects it.
        """
        assert predicate, "Expected a callable predicate"  # type: ignore[truthy-function]
        self.predicate = predicate

    def __call__(self, prefix: str, **kwargs: _Ignored) -> Generator[str]:
        """
        Provide completions on prefix
        """
        # Expand a leading "~" so that filesystem operations below work on a
        # real path (os.listdir("~") would otherwise raise FileNotFoundError).
        expanded_prefix = os.path.expanduser(prefix)
        target_dir = os.path.dirname(expanded_prefix)
        try:
            names = os.listdir(target_dir or ".")
        except Exception:
            return  # empty iterator
        incomplete_part = os.path.basename(expanded_prefix)
        # Iterate on target_dir entries and filter on given predicate
        for name in names:
            if not name.startswith(incomplete_part):
                continue
            candidate = os.path.join(target_dir, name)
            if not self.predicate(candidate):
                continue
            yield candidate + "/" if os.path.isdir(candidate) else candidate


class DirectoriesCompleter(_FilteredFilesCompleter):
    def __init__(self) -> None:
        _FilteredFilesCompleter.__init__(self, predicate=os.path.isdir)


class SuppressCompleter(BaseCompleter):
    """
    A completer used to suppress the completion of specific arguments
    """

    def __init__(self) -> None:
        pass

    def suppress(self) -> bool:
        """
        Decide if the completion should be suppressed
        """
        return True
