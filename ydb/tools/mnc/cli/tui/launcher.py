import asyncio
import contextlib
import io
import shlex
from dataclasses import dataclass
from typing import Any, List, Optional

from ydb.tools.mnc.cli import arg_metadata, command_options
from ydb.tools.mnc.cli.tui.command_picker import CommandPickerApp
from ydb.tools.mnc.cli.tui.common import (
    COMMON_OPTION_GROUP,
    SKIPPED_OPTION_DESTS,
    discover_config_candidates,
    primary_alias,
    value_to_text,
)
from ydb.tools.mnc.cli.tui.config_picker import ConfigPickerApp
from ydb.tools.mnc.cli.tui.options_form import OptionsFormApp


@dataclass
class LauncherResult:
    args: Any = None
    argv: Optional[List[str]] = None
    cancelled: bool = False


def should_route_to_launcher(args, expected_config_by_verb, prefer_launcher_by_verb=None) -> bool:
    if getattr(args, 'verb', None) is None:
        return True
    if not getattr(args, 'tui', False):
        return False
    if expected_config_by_verb.get(args.verb) and not getattr(args, 'config_name', None) and not getattr(args, 'config_path', None):
        return True
    return False


class TuiLauncher:
    def __init__(self, parser, expected_config_by_verb):
        self.parser = parser
        self.expected_config_by_verb = expected_config_by_verb
        self.root = arg_metadata.command_metadata_from_parser(parser)

    def run(self, initial_args=None, initial_argv: Optional[List[str]] = None) -> LauncherResult:
        return asyncio.run(self.run_async(initial_args, initial_argv=initial_argv))

    async def run_async(self, initial_args=None, initial_argv: Optional[List[str]] = None) -> LauncherResult:
        command = await self._select_command(initial_args)
        if command is None:
            return LauncherResult(cancelled=True)

        initial_args = self._initial_args_with_cached_options(command, initial_args, initial_argv)
        argv = self._global_argv(initial_args) + list(command.path)

        command_scheme = self.expected_config_by_verb.get(command.path[0])
        if command_scheme:
            config_arg = await self._select_config(initial_args, command_scheme)
            if config_arg is None:
                return LauncherResult(cancelled=True)
            argv.extend(config_arg)

        common_values = await OptionsFormApp(
            "Configure common options",
            initial_args,
            options=self._common_options(command),
        ).run_async()
        if common_values is None:
            return LauncherResult(cancelled=True)
        argv.extend(self._values_to_argv(command, common_values, initial_args))

        command_values = await OptionsFormApp(
            "Configure command options",
            initial_args,
            options=self._command_options(command),
            arguments=command.arguments,
        ).run_async()
        if command_values is None:
            return LauncherResult(cancelled=True)
        argv.extend(self._values_to_argv(command, command_values, initial_args))

        return LauncherResult(args=self.parser.parse_args(argv), argv=argv)

    async def _select_command(self, initial_args) -> Optional[arg_metadata.CommandMeta]:
        command = command_from_args(self.root, initial_args)
        if command is not None and command.is_leaf:
            return command

        return await CommandPickerApp(self.root, initial=command or self.root).run_async()

    async def _select_config(self, initial_args, command_scheme) -> Optional[List[str]]:
        if getattr(initial_args, "config_name", None):
            return ["--config", initial_args.config_name]
        if getattr(initial_args, "config_path", None):
            return ["--config-path", initial_args.config_path]
        selected = await ConfigPickerApp(discover_config_candidates(), command_scheme=command_scheme).run_async()
        if selected is None:
            return None
        return [selected[0], selected[1]]

    def _global_argv(self, initial_args) -> List[str]:
        argv = []
        if getattr(initial_args, "verbose", False):
            argv.append("--verbose")
        if getattr(initial_args, "quiet", False):
            argv.append("--quiet")
        return argv

    def _common_options(self, command):
        return [
            option for option in command.options
            if option.group == COMMON_OPTION_GROUP and option.dest not in SKIPPED_OPTION_DESTS
        ]

    def _command_options(self, command):
        return [
            option for option in command.options
            if option.group != COMMON_OPTION_GROUP and option.dest not in SKIPPED_OPTION_DESTS
        ]

    def _values_to_argv(self, command, values, initial_args) -> List[str]:
        argv = []
        options_by_dest = {option.dest: option for option in command.options}
        arguments_by_dest = {argument.dest: argument for argument in command.arguments}

        for dest, value in values["flags"].items():
            option = options_by_dest[dest]
            default = bool(option.default)
            if bool(value) != default:
                argv.append(primary_alias(option))

        for dest, value in values["options"].items():
            option = options_by_dest[dest]
            if value == "" or value == []:
                continue
            if value == value_to_text(option.default):
                continue
            argv.append(primary_alias(option))
            if option.multivalue:
                if isinstance(value, list):
                    argv.extend(value)
                else:
                    argv.extend(shlex.split(value))
            else:
                argv.append(value)

        for dest, value in values["arguments"].items():
            argument = arguments_by_dest[dest]
            if value == "":
                continue
            if argument.multivalue:
                argv.extend(shlex.split(value))
            else:
                argv.append(value)
        return argv

    def _initial_args_with_cached_options(self, command, initial_args, initial_argv: Optional[List[str]] = None):
        if initial_argv is not None:
            path, _, _ = command_options.command_from_argv(self.parser, initial_argv)
            if path == command.path:
                return self._parse_initial_args(
                    command_options.apply_cached_options(self.parser, initial_argv),
                    initial_args,
                )

        cache_entry = command_options.load_cache().get(command_options.command_key(command.path), {})
        cached_tokens = cache_entry.get("tokens", [])
        if not isinstance(cached_tokens, list) or not cached_tokens:
            return initial_args

        argv = self._global_argv(initial_args) + list(command.path) + cached_tokens
        return self._parse_initial_args(argv, initial_args)

    def _parse_initial_args(self, argv: List[str], fallback):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                return self.parser.parse_args(argv)
        except (Exception, SystemExit):
            return fallback


def command_from_args(root: arg_metadata.CommandMeta, args) -> Optional[arg_metadata.CommandMeta]:
    if args is None:
        return None
    command = root
    while command.parser._subparsers is not None:
        dest = command.parser._subparsers._value.metainfo.name
        name = getattr(args, dest, None)
        if name is None:
            return command if command is not root else None
        next_command = None
        for child in command.children:
            if child.name == name:
                next_command = child
                break
        if next_command is None:
            return None
        command = next_command
    return command
