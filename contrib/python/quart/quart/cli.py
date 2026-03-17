from __future__ import annotations

import asyncio
import code
import functools
import os
import sys
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, List, Optional, TYPE_CHECKING

import click

from .helpers import get_debug_flag

try:
    from importlib.metadata import version
except ModuleNotFoundError:
    from importlib_metadata import version  # type: ignore

try:
    from dotenv import load_dotenv
except ImportError:
    pass

__version__ = version("quart")

if TYPE_CHECKING:
    from .app import Quart  # noqa: F401


class NoAppException(click.UsageError):
    def __init__(self) -> None:
        super().__init__(
            "Could not locate a Quart application as the QUART_APP environment "
            "variable has either not been set or provided or does not point to "
            "a valid application.\n"
            "Please set it to module_name:app_name or module_name:factory_function()\n"
            "Note `quart` is not a valid module_name."
        )


class ScriptInfo:
    def __init__(
        self, app_import_path: Optional[str] = None, create_app: Optional[Callable] = None
    ) -> None:
        self.load_dotenv_if_exists()
        self.app_import_path = app_import_path or os.environ.get("QUART_APP")
        self.create_app = create_app
        self.data: dict = {}
        self._app: Optional["Quart"] = None

    def load_app(self) -> "Quart":
        if self._app is None:
            if self.create_app is not None:
                self._app = self.create_app()
            else:
                try:
                    module_name, app_name = self.app_import_path.split(":", 1)
                except ValueError:
                    module_name, app_name = self.app_import_path, "app"
                except AttributeError as error:
                    raise NoAppException() from error

                module_path = Path(module_name).resolve()
                sys.path.insert(0, str(module_path.parent))
                if module_path.is_file():
                    import_name = module_path.with_suffix("").name
                else:
                    import_name = module_path.name
                try:
                    module = import_module(import_name)
                except ModuleNotFoundError as error:
                    if error.name == import_name:
                        raise NoAppException() from error
                    else:
                        raise

                try:
                    self._app = eval(app_name, vars(module))
                except NameError as error:
                    raise NoAppException() from error

                from .app import Quart

                if not isinstance(self._app, Quart):
                    self._app = None
                    raise NoAppException()

        if self._app is None:
            raise NoAppException()

        self._app.debug = get_debug_flag()

        return self._app

    def load_dotenv_if_exists(self) -> None:
        if os.environ.get("QUART_SKIP_DOTENV") == "1":
            return

        if not Path(".env").is_file() and not Path(".quartenv").is_file():
            return

        try:
            if Path(".env").is_file():
                load_dotenv()
            if Path(".quartenv").is_file():
                load_dotenv(dotenv_path=Path(".") / ".quartenv")
        except NameError:
            print(  # noqa: T001, T002
                "* Tip: There are .env files present. "
                'Do "pip install python-dotenv" to use them.'
            )


pass_script_info = click.make_pass_decorator(ScriptInfo, ensure=True)


def with_appcontext(fn: Optional[Callable] = None) -> Callable:
    # decorator was used with parenthesis
    if fn is None:
        return with_appcontext

    @click.pass_context
    def decorator(__ctx: click.Context, *args: Any, **kwargs: Any) -> Any:
        async def _inner() -> Any:
            async with __ctx.ensure_object(ScriptInfo).load_app().app_context():
                try:
                    return __ctx.invoke(fn, *args, **kwargs)
                except RuntimeError as error:
                    if error.args[0] == "Cannot run the event loop while another loop is running":
                        print(  # noqa: T001, T002
                            "The appcontext cannot be used with a command that runs an event loop. "
                            "See quart#361 for more details"
                        )
                    raise

        return asyncio.run(_inner())

    return functools.update_wrapper(decorator, fn)


class AppGroup(click.Group):
    """This works similar to a regular click :class:`~click.Group` but it
    changes the behavior of the :meth:`command` decorator so that it
    automatically wraps the functions in :func:`with_appcontext`.

    Not to be confused with :class:`QuartGroup`.
    """

    def command(self, *args: Any, **kwargs: Any) -> Callable:
        """This works exactly like the method of the same name on a regular
        :class:`click.Group` but it wraps callbacks in :func:`with_appcontext`
        if it's enabled by passing ``with_appcontext=True``.
        """
        wrap_for_ctx = kwargs.pop("with_appcontext", False)

        def decorator(f: Callable) -> Callable:
            if wrap_for_ctx:
                f = with_appcontext(f)
            return click.Group.command(self, *args, **kwargs)(f)

        return decorator

    def group(self, *args: Any, **kwargs: Any) -> Callable:
        kwargs.setdefault("cls", AppGroup)
        return super().group(*args, **kwargs)


def get_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    message = f"Quart {__version__}"
    click.echo(message, color=ctx.color)
    ctx.exit()


version_option = click.Option(
    ["--version"],
    help="Show the Quart version",
    expose_value=False,
    callback=get_version,
    is_flag=True,
    is_eager=True,
)


class QuartGroup(AppGroup):
    def __init__(
        self,
        add_default_commands: bool = True,
        create_app: Optional[Callable] = None,
        add_version_option: bool = True,
        *,
        params: Optional[List] = None,
        **kwargs: Any,
    ) -> None:
        params = params or []
        if add_version_option:
            params.append(version_option)
        super().__init__(params=params, **kwargs)
        self.create_app = create_app

        if add_default_commands:
            self.add_command(run_command)
            self.add_command(shell_command)
            self.add_command(routes_command)

    def get_command(self, ctx: click.Context, name: str) -> click.Command:
        """Return the relevant command given the context and name.

        .. warning::

            This differs substantially from Flask in that it allows
            for the inbuilt commands to be overridden.
        """
        info = ctx.ensure_object(ScriptInfo)
        command = None
        try:
            command = info.load_app().cli.get_command(ctx, name)  # type: ignore
        except NoAppException:
            pass
        if command is None:
            command = super().get_command(ctx, name)
        return command

    def list_commands(self, ctx: click.Context) -> List[str]:
        commands = set(click.Group.list_commands(self, ctx))
        info = ctx.ensure_object(ScriptInfo)
        commands.update(info.load_app().cli.list_commands(ctx))  # type: ignore
        return list(commands)

    def main(self, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("obj", ScriptInfo(create_app=self.create_app))
        return super().main(*args, **kwargs)


@click.command("run", short_help="Start and run a development server.")
@click.option("--host", "-h", default="127.0.0.1", help="The interface to serve on.")
@click.option("--port", "-p", default=5000, help="The port to serve on.")
@click.option(
    "--certfile",
    "--cert",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path of the certificate",
)
@click.option(
    "--keyfile",
    "--key",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path of the key",
)
@pass_script_info
def run_command(info: ScriptInfo, host: str, port: int, certfile: str, keyfile: str) -> None:
    debug = get_debug_flag()
    app = info.load_app()
    app.run(
        debug=debug, host=host, port=port, certfile=certfile, keyfile=keyfile, use_reloader=True
    )


@click.command("shell", short_help="Open a shell within the app context.")
@pass_script_info
def shell_command(info: ScriptInfo) -> None:
    app = info.load_app()
    context = {}
    context.update(app.make_shell_context())

    banner = f"Python {sys.version} on {sys.platform} running {app.import_name}"
    code.interact(banner=banner, local=context)


@click.command("routes", short_help="Show this app's routes.")
@click.option(
    "--sort",
    "-s",
    type=click.Choice({"endpoint", "methods", "rule", "match"}),  # type: ignore
    default="endpoint",
    help="Order the routes by type, 'match' is the matching order when dispatching a request.",
)
@click.option("--all-methods", is_flag=True, help="Show HEAD and OPTIONS methods.")
@pass_script_info
def routes_command(info: ScriptInfo, sort: str, all_methods: bool) -> None:
    app = info.load_app()
    rules = list(app.url_map.iter_rules())
    if len(rules) == 0:
        click.echo("No routes were registered.")
        return

    ignored_methods = set() if all_methods else {"HEAD", "OPTIONS"}

    if sort == "endpoint":
        rules = sorted(rules, key=lambda rule: rule.endpoint)
    elif sort == "rule":
        rules = sorted(rules, key=lambda rule: rule.rule)
    elif sort == "methods":
        rules = sorted(rules, key=lambda rule: sorted(rule.methods))

    headers = ("Endpoint", "Methods", "Websocket", "Rule")
    rule_methods = [", ".join(sorted(rule.methods - ignored_methods)) for rule in rules]
    widths = (
        max(len(rule.endpoint) for rule in rules),
        max(len(methods) for methods in rule_methods),
        len("Websocket"),
        max(len(rule.rule) for rule in rules),
    )
    widths = [max(len(header), width) for header, width in zip(headers, widths)]
    row = "{{0:<{0}}} | {{1:<{1}}} | {{2:<{2}}} | {{3:<{3}}}".format(*widths)

    click.echo(row.format(*headers).strip())
    click.echo(row.format(*("-" * width for width in widths)))

    for rule, methods in zip(rules, rule_methods):
        click.echo(row.format(rule.endpoint, methods, str(rule.websocket), rule.rule).rstrip())


cli = QuartGroup(
    help="""\
Utility functions for Quart applications.

This will load the app defined in the QUART_APP environment
variable. The QUART_APP variable follows the Gunicorn standard of
`module_name:application_name` e.g. `hello:app`.

\b
{prefix}{cmd} QUART_APP=hello:app
{prefix}{cmd} QUART_DEBUG=1
{prefix}quart run
    """.format(
        cmd="export" if os.name == "posix" else "set", prefix="$ " if os.name == "posix" else "> "
    )
)


def main(as_module: bool = False) -> None:
    args = sys.argv[1:]

    if as_module:
        name = "python -m quart"
        sys.argv = ["-m", "quart"] + args
    else:
        name = None

    cli.main(args=args, prog_name=name)


if __name__ == "__main__":
    main(as_module=True)
