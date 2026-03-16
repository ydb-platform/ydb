from __future__ import annotations

import argparse
import asyncio
import functools
import json
import logging
import os
import shlex
import sys
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import procrastinate
from procrastinate import connector, exceptions, jobs, shell, types, utils

logger = logging.getLogger(__name__)

PROGRAM_NAME = "procrastinate"
ENV_PREFIX = PROGRAM_NAME.upper()


def get_log_level(verbosity: int) -> int:
    """
    Given the number of repetitions of the flag -v,
    returns the desired log level
    """
    return {0: logging.INFO, 1: logging.DEBUG}.get(min((1, verbosity)), 0)


Style = Literal["%", "{", "$"]


def configure_logging(verbosity: int, format: str, style: Style) -> None:
    level = get_log_level(verbosity=verbosity)
    logging.basicConfig(level=level, format=format, style=style)
    level_name = logging.getLevelName(level)
    logger.debug(
        f"Log level set to {level_name}",
        extra={"action": "set_log_level", "value": level_name},
    )


def print_stderr(*args: Any):
    print(*args, file=sys.stderr)


class MissingAppConnector(connector.BaseAsyncConnector):
    def get_sync_connector(self) -> connector.BaseConnector:
        return self

    def open(self, *args: Any, **kwargs: Any):
        pass

    def close(self, *args: Any, **kwargs: Any):
        pass

    async def open_async(self, *args: Any, **kwargs: Any):
        pass

    async def close_async(self, *args: Any, **kwargs: Any):
        pass

    def execute_query(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp

    def execute_query_one(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp

    def execute_query_all(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp

    async def execute_query_async(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp

    async def execute_query_one_async(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp

    async def execute_query_all_async(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp

    async def listen_notify(self, *args: Any, **kwargs: Any):
        raise exceptions.MissingApp


class ActionWithNegative(argparse._StoreTrueAction):  # pyright: ignore[reportPrivateUsage]
    def __init__(self, *args: Any, negative: str | None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.negative = negative

    def __call__(self, parser: Any, ns: Any, values: Any, option: Any = None):
        if self.negative is None:
            setattr(ns, self.dest, option and not option.startswith("--no-"))
            return
        setattr(ns, self.dest, option != self.negative)


def store_true_with_negative(negative: str | None = None):
    """
    Return an argparse action that works like store_true but also accepts
    a flag to set the value to False. By default, any flag starting with
    `--no-` will set the value to False. If a string is provided as the
    `negative` argument, this flags will set the value to False.
    """
    return functools.partial(ActionWithNegative, negative=negative)


def load_app(app_path: str) -> procrastinate.App:
    if app_path == "":
        # If we don't provide an app, initialize a default one that will fail if it
        # needs a connector.
        return procrastinate.App(connector=MissingAppConnector())

    try:
        app = procrastinate.App.from_path(dotted_path=app_path)
    except exceptions.LoadFromPathError as exc:
        raise argparse.ArgumentError(
            None, f"Could not load app from {app_path}"
        ) from exc
    if not isinstance(app.connector, connector.BaseAsyncConnector):
        raise argparse.ArgumentError(
            None,
            "The connector provided by the app is not async. "
            "Please use an async connector for the procrastinate CLI.",
        )
    return app


def cast_queues(queues: str) -> list[str] | None:
    cleaned_queues = (queue.strip() for queue in queues.split(","))
    return [queue for queue in cleaned_queues if queue] or None


def env_bool(value: str) -> bool:
    value = value.lower()[0]
    if value in ["0", "n", "f"]:
        return False
    elif value in ["1", "y", "t"]:
        return True
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid environment variable value for boolean: {value!r}. "
            f"Expected one of 0/n/f or 1/y/t"
        )


def add_argument(
    parser: argparse._ActionsContainer,  # pyright: ignore[reportPrivateUsage]
    *args: Any,
    envvar: str | None = None,
    envvar_help: str = "",
    envvar_type: Callable[..., Any] | None = None,
    **kwargs: Any,
):
    if envvar is not None:
        envvar = f"{ENV_PREFIX}_{envvar.upper()}"
        # Empty string is considered as unset
        default = kwargs.get("default") or None
        env_var_default = os.environ.get(envvar, default)
        envvar_type = envvar_type or kwargs.get("type")
        if env_var_default not in ["", None, argparse.SUPPRESS] and envvar_type:
            env_var_default = envvar_type(env_var_default)

        if env_var_default:
            kwargs["default"] = env_var_default

        kwargs["help"] = (
            f"{kwargs.get('help', '')} "
            f"(env: {envvar}{f': {envvar_help}' if envvar_help else ''})"
        )
    return parser.add_argument(*args, **kwargs)


def add_cli_features(parser: argparse.ArgumentParser):
    """
    Add features to the parser to make it more CLI-friendly.
    This is not necessary when the parser is used as a subparser.
    """
    add_argument(
        parser,
        "-v",
        "--verbose",
        default=0,
        action="count",
        help="Use multiple times to increase verbosity",
        envvar="VERBOSE",
        envvar_help="set to desired verbosity level",
        envvar_type=int,
    )
    log_group = parser.add_argument_group("Logging")
    add_argument(
        log_group,
        "--log-format",
        default=logging.BASIC_FORMAT,
        help="Defines the format used for logging (see "
        "https://docs.python.org/3/library/logging.html#logrecord-attributes)",
        envvar="LOG_FORMAT",
    )
    add_argument(
        log_group,
        "--log-format-style",
        default="%",
        choices=["%", "{", "$"],
        help="Defines the style for the log format string (see "
        "https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles)",
        envvar="LOG_FORMAT_STYLE",
    )
    add_argument(
        parser,
        "-V",
        "--version",
        action="version",
        help="Print the version and exit",
        version=f"%(prog)s, version {procrastinate.__version__}",
    )


parser_options = {
    "allow_abbrev": False,
    "formatter_class": argparse.ArgumentDefaultsHelpFormatter,
}


def create_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        description="Interact with a Procrastinate app. See subcommands for details.",
        **parser_options,  # pyright: ignore[reportArgumentType]
    )


def add_arguments(
    parser: argparse.ArgumentParser,
    include_app: bool = True,
    include_schema: bool = True,
    custom_healthchecks: Callable[[procrastinate.App], Awaitable[None]] | None = None,
):
    if include_app:
        add_argument(
            parser,
            "-a",
            "--app",
            default="",
            type=load_app,
            help="Dotted path to the Procrastinate app",
            envvar="APP",
        )

    subparsers = parser.add_subparsers(dest="command", required=True)
    configure_worker_parser(subparsers)
    configure_defer_parser(subparsers)
    if include_schema:
        configure_schema_parser(subparsers)
    configure_healthchecks_parser(subparsers, custom_healthchecks=custom_healthchecks)
    configure_shell_parser(subparsers)
    return parser


def configure_worker_parser(subparsers: argparse._SubParsersAction[Any]):  # pyright: ignore[reportPrivateUsage]
    # --- Worker ---
    worker_parser = subparsers.add_parser(
        "worker",
        help="Launch a worker, listening on the given queues (or all queues). "
        "Values default to App.worker_defaults and then App.run_worker() defaults values.",
        argument_default=argparse.SUPPRESS,
        **parser_options,  # pyright: ignore[reportArgumentType]
    )
    worker_parser.set_defaults(func=worker_)
    add_argument(
        worker_parser,
        "-n",
        "--name",
        help="Name of the worker",
        envvar="WORKER_NAME",
    )
    add_argument(
        worker_parser,
        "-q",
        "--queues",
        type=cast_queues,
        help="Comma-separated names of the queues to listen "
        "to (empty string for all queues)",
        envvar="WORKER_QUEUES",
    )
    add_argument(
        worker_parser,
        "-c",
        "--concurrency",
        type=int,
        help="Number of parallel asynchronous jobs to process at once",
        envvar="WORKER_CONCURRENCY",
    )
    add_argument(
        worker_parser,
        "-p",
        "--fetch-job-polling-interval",
        type=float,
        help="How long to wait for database event push before polling",
        envvar="WORKER_FETCH_JOB_POLLING_INTERVAL",
    )
    add_argument(
        worker_parser,
        "-a",
        "--abort-job-polling-interval",
        type=float,
        help="How often to polling for abort requests",
        envvar="WORKER_ABORT_JOB_POLLING_INTERVAL",
    )
    add_argument(
        worker_parser,
        "--shutdown-graceful-timeout",
        type=float,
        help="How long to wait for jobs to complete when shutting down before aborting them",
        envvar="WORKER_SHUTDOWN_GRACEFUL_TIMEOUT",
    )
    add_argument(
        worker_parser,
        "-w",
        "--wait",
        "--one-shot",
        action=store_true_with_negative("--one-shot"),
        help="When all jobs have been processed, whether to "
        "terminate or to wait for new jobs",
        envvar="WORKER_WAIT",
        envvar_help="use 0/n/f or 1/y/t",
        envvar_type=env_bool,
    )
    add_argument(
        worker_parser,
        "-l",
        "--listen-notify",
        "--no-listen-notify",
        action=store_true_with_negative(),
        help="Whether to actively listen for new jobs or periodically poll",
        envvar="WORKER_LISTEN_NOTIFY",
        envvar_help="use 0/n/f or 1/y/t",
        envvar_type=env_bool,
    )
    add_argument(
        worker_parser,
        "--delete-jobs",
        choices=jobs.DeleteJobCondition,
        type=jobs.DeleteJobCondition,
        help="If set, delete jobs on completion",
        envvar="WORKER_DELETE_JOBS",
    )


def configure_defer_parser(subparsers: argparse._SubParsersAction[Any]):  # pyright: ignore[reportPrivateUsage]
    # --- Defer ---
    defer_parser = subparsers.add_parser(
        "defer",
        help="Create a job from the given task, to be executed by a worker. "
        "TASK should be the name or dotted path to a task. "
        "JSON_ARGS should be a json object (a.k.a dictionary) with the job parameters",
        **parser_options,  # pyright: ignore[reportArgumentType]
    )
    defer_parser.set_defaults(func=defer)
    add_argument(
        defer_parser,
        "task",
        help="Name or dotted path to the task to defer",
    )
    add_argument(
        defer_parser,
        "json_args",
        nargs="?",
        # We can't cast it right away into json, because we need to use
        # the app's json loads function.
        help="JSON object with the job parameters",
        envvar="DEFER_JSON_ARGS",
    )
    add_argument(
        defer_parser,
        "--queue",
        default=argparse.SUPPRESS,
        help="The queue for deferring. Defaults to the task's default queue",
        envvar="DEFER_QUEUE",
    )
    add_argument(
        defer_parser,
        "--lock",
        default=argparse.SUPPRESS,
        help="A lock string. Jobs sharing the same lock will not run concurrently",
        envvar="DEFER_LOCK",
    )
    add_argument(
        defer_parser,
        "--queueing-lock",
        default=argparse.SUPPRESS,
        help="A string value. The defer operation will fail if there already is a job "
        "waiting in the queue with the same queueing lock",
        envvar="DEFER_QUEUEING_LOCK",
    )
    add_argument(
        defer_parser,
        "-i",
        "--ignore-already-enqueued",
        "--no-ignore-already-enqueued",
        action=store_true_with_negative(),
        default=False,
        help="Exit with code 0 even if the queueing lock is already taken, while still "
        "displaying an error",
        envvar="DEFER_IGNORE_ALREADY_ENQUEUED",
        envvar_help="use 0/n/f or 1/y/t",
        envvar_type=env_bool,
    )
    time_group = defer_parser.add_mutually_exclusive_group()
    add_argument(
        time_group,
        "--at",
        default=argparse.SUPPRESS,
        dest="schedule_at",
        type=utils.parse_datetime,
        help="ISO-8601 localized datetime after which to launch the job",
        envvar="DEFER_AT",
    )
    add_argument(
        time_group,
        "--in",
        default=argparse.SUPPRESS,
        dest="schedule_in",
        type=lambda s: {"seconds": int(s)},
        help="Number of seconds after which to launch the job",
        envvar="DEFER_IN",
    )
    add_argument(
        defer_parser,
        "--priority",
        default=argparse.SUPPRESS,
        dest="priority",
        type=int,
        help="Job priority. May be positive or negative (higher values indicate jobs "
        "that should execute first). Defaults to 0",
        envvar="DEFER_PRIORITY",
    )
    add_argument(
        defer_parser,
        "--unknown",
        "--no-unknown",
        action=store_true_with_negative(),
        default=False,
        help="Whether unknown tasks can be deferred",
        envvar="DEFER_UNKNOWN",
        envvar_help="use 0/n/f or 1/y/t",
        envvar_type=env_bool,
    )


def configure_schema_parser(subparsers: argparse._SubParsersAction[Any]):  # pyright: ignore[reportPrivateUsage]
    # --- Schema ---
    schema_parser = subparsers.add_parser(
        "schema",
        help="Apply SQL schema to the empty database. This won't work if the schema has already "
        "been applied.",
        **parser_options,  # pyright: ignore[reportArgumentType]
    )
    schema_parser.set_defaults(func=schema)
    add_argument(
        schema_parser,
        "--apply",
        action="store_const",
        const="apply",
        dest="action",
        help="Apply the schema to the DB (default)",
    )
    add_argument(
        schema_parser,
        "--read",
        action="store_const",
        const="read",
        dest="action",
        help="Read the schema SQL and output it",
    )
    add_argument(
        schema_parser,
        "--migrations-path",
        action="store_const",
        const="migrations_path",
        dest="action",
        help="Output the path to the directory containing the migration scripts",
    )


def configure_healthchecks_parser(
    subparsers: argparse._SubParsersAction[Any],  # pyright: ignore[reportPrivateUsage],
    custom_healthchecks: Callable[[procrastinate.App], Awaitable[None]] | None = None,
):
    # --- Healthchecks ---
    healthchecks_parser = subparsers.add_parser(
        "healthchecks",
        help="Check the state of procrastinate",
        **parser_options,  # pyright: ignore[reportArgumentType],
    )
    healthchecks_parser.set_defaults(func=custom_healthchecks or healthchecks)


def configure_shell_parser(subparsers: argparse._SubParsersAction[Any]):  # pyright: ignore[reportPrivateUsage]
    # --- Shell ---
    shell_parser = subparsers.add_parser(
        "shell",
        help="Administration shell for procrastinate",
        **parser_options,  # pyright: ignore[reportArgumentType]
    )
    add_argument(
        shell_parser,
        "shell_command",
        nargs="*",
        help="Invoke a shell command and exit",
    )
    shell_parser.set_defaults(func=shell_)


async def cli(args: list[str]):
    parser = create_parser()
    add_arguments(parser)
    add_cli_features(parser)
    parsed = vars(parser.parse_args(args))

    configure_logging(
        verbosity=parsed.pop("verbose"),
        format=parsed.pop("log_format"),
        style=parsed.pop("log_format_style"),
    )
    await execute_command(parsed)


async def execute_command(parsed: dict[str, Any]):
    parsed.pop("command")
    try:
        async with parsed.pop("app").open_async() as app:
            # Before calling the subcommand function,
            # we want to have popped all top-level arguments
            # from the parsed dict and kept only the subcommand
            # arguments.
            await parsed.pop("func")(app=app, **parsed)
    except Exception as exc:
        logger.debug("Exception details:", exc_info=exc)
        messages = [str(e) for e in utils.causes(exc)]
        exit_message = "\n".join(e.strip() for e in messages[::-1] if e)

        print_stderr(exit_message)
        sys.exit(1)


async def worker_(
    app: procrastinate.App,
    **kwargs: Any,
):
    """
    Launch a worker, listening on the given queues (or all queues).
    Values default to App.worker_defaults and then App.run_worker() defaults values.
    """
    queues = kwargs.get("queues")
    print_stderr(
        f"Launching a worker on {'all queues' if not queues else ', '.join(queues)}"
    )
    await app.run_worker_async(**kwargs)


async def defer(
    app: procrastinate.App,
    task: str,
    json_args: str | None,
    ignore_already_enqueued: bool,
    unknown: bool,
    **configure_kwargs: Any,
):
    """
    Create a job from the given task, to be executed by a worker.
    TASK should be the name or dotted path to a task.
    JSON_ARGS should be a json object (a.k.a dictionary) with the job parameters
    """
    # Loading json args
    args = (
        load_json_args(
            json_args=json_args, json_loads=app.connector.json_loads or json.loads
        )
        if json_args
        else {}
    )
    # Configure the job. If the task is known, it will be used.
    job_deferrer = configure_task(
        app=app,
        task_name=task,
        configure_kwargs=configure_kwargs,
        allow_unknown=unknown,
    )

    # Printing info
    str_kwargs = ", ".join(f"{k}={v!r}" for k, v in args.items())
    print_stderr(f"Launching a job: {task}({str_kwargs})")
    # And launching the job
    try:
        await job_deferrer.defer_async(**args)  # type: ignore
    except exceptions.AlreadyEnqueued as exc:
        if not ignore_already_enqueued:
            raise
        print_stderr(f"{exc} (ignored)")


def load_json_args(json_args: str | None, json_loads: Callable) -> types.JSONDict:
    if json_args is None:
        return {}
    else:
        try:
            args = json_loads(json_args)
            assert isinstance(args, dict)
        except Exception as exc:
            raise ValueError(
                "Incorrect JSON_ARGS value expecting a valid json object (or dict)"
            ) from exc
    return args


def configure_task(
    app: procrastinate.App,
    task_name: str,
    configure_kwargs: dict[str, Any],
    allow_unknown: bool,
) -> jobs.JobDeferrer:
    return app.configure_task(
        name=task_name, allow_unknown=allow_unknown, **configure_kwargs
    )


async def schema(app: procrastinate.App, action: str):
    """
    Apply SQL schema to the empty database. This won't work if the schema has already
    been applied.
    """
    action = action or "apply"
    schema_manager = app.schema_manager
    if action == "apply":
        print_stderr("Applying schema")
        await schema_manager.apply_schema_async()
        print_stderr("Done")
    elif action == "read":
        print(schema_manager.get_schema().strip())
    else:
        print(schema_manager.get_migrations_path())


async def healthchecks(app: procrastinate.App):
    """
    Check the state of procrastinate.
    """
    db_ok = await app.check_connection_async()
    # If app or DB is not configured correctly, we raise before this point
    print("App configuration: OK")
    print("DB connection: OK")

    if not db_ok:
        raise RuntimeError(
            "Connection to the database works but the procrastinate_jobs table was not "
            "found. Have you applied database migrations (see "
            "`procrastinate schema -h`)?"
        )

    print("Found procrastinate_jobs table: OK")


async def shell_(app: procrastinate.App, shell_command: list[str]):
    """
    Administration shell for procrastinate.
    """
    shell_obj = shell.ProcrastinateShell(
        job_manager=app.job_manager,
    )

    if shell_command:
        await utils.sync_to_async(shell_obj.onecmd, line=shlex.join(shell_command))
    else:
        await utils.sync_to_async(shell_obj.cmdloop)


def main():
    kwargs = {}
    if os.name == "nt":
        if sys.version_info < (3, 14):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        else:
            kwargs["loop_factory"] = asyncio.SelectorEventLoop

    asyncio.run(cli(sys.argv[1:]), **kwargs)
