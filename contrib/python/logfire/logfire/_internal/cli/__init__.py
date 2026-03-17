"""The CLI for Pydantic Logfire."""

from __future__ import annotations

import argparse
import functools
import logging
import platform
import sys
import warnings
from collections.abc import Sequence
from operator import itemgetter
from pathlib import Path
from typing import Any

import requests
from opentelemetry import trace
from rich.console import Console

from logfire.exceptions import LogfireConfigError
from logfire.propagate import ContextCarrier, get_context

from ...version import VERSION
from ..auth import HOME_LOGFIRE
from ..client import LogfireClient
from ..config import REGIONS, LogfireCredentials, get_base_url_from_token
from ..config_params import ParamManager
from ..tracer import SDKTracerProvider
from .auth import parse_auth
from .prompt import parse_prompt
from .run import collect_instrumentation_context, parse_run, print_otel_summary

BASE_OTEL_INTEGRATION_URL = 'https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/'
BASE_DOCS_URL = 'https://logfire.pydantic.dev/docs'
INTEGRATIONS_DOCS_URL = f'{BASE_DOCS_URL}/integrations/'
LOGFIRE_LOG_FILE = HOME_LOGFIRE / 'log.txt'

logger = logging.getLogger(__name__)
__all__ = 'main', 'logfire_info'


def version_callback() -> None:
    """Show the version and exit."""
    py_impl = platform.python_implementation()
    py_version = platform.python_version()
    system = platform.system()
    print(f'Running Logfire {VERSION} with {py_impl} {py_version} on {system}.')


def parse_whoami(args: argparse.Namespace) -> None:
    """Show user authenticated username and the URL to your Logfire project."""
    data_dir = Path(args.data_dir)
    param_manager = ParamManager.create(data_dir)
    base_url: str | None = param_manager.load_param('base_url', args.logfire_url)
    tokens = param_manager.load_param('token')

    if tokens:
        # Display info for all configured tokens
        any_succeeded = False
        for i, token in enumerate(tokens):
            if len(tokens) > 1:
                if i > 0:
                    sys.stderr.write('\n')
                sys.stderr.write(f'Token {i + 1} of {len(tokens)}:\n')
            token_base_url = base_url or get_base_url_from_token(token)
            credentials = LogfireCredentials.from_token(token, args._session, token_base_url)
            if credentials:
                credentials.print_token_summary()
                any_succeeded = True
        if any_succeeded:
            return
        # If no tokens yielded credentials, fall through to try creds file

    try:
        client = LogfireClient.from_url(base_url)
    except LogfireConfigError:
        sys.stderr.write('Not logged in. Run `logfire auth` to log in.\n')
    else:
        current_user = client.get_user_information()
        username = current_user['name']
        sys.stderr.write(f'Logged in as: {username}\n')

    credentials = LogfireCredentials.load_creds_file(data_dir)
    if credentials is None:
        sys.stderr.write(f'No Logfire credentials found in {data_dir.resolve()}\n')
        sys.exit(1)
    else:
        sys.stderr.write(f'Credentials loaded from data dir: {data_dir.resolve()}\n\n')
        credentials.print_token_summary()


def parse_clean(args: argparse.Namespace) -> None:
    """Remove the contents of the Logfire data directory."""
    files_to_delete: list[Path] = []
    if args.logs and LOGFIRE_LOG_FILE.exists():
        files_to_delete.append(LOGFIRE_LOG_FILE)

    data_dir = Path(args.data_dir)
    if not data_dir.exists() or not data_dir.is_dir():
        sys.stderr.write(f'No Logfire data found in {data_dir.resolve()}\n')
        sys.exit(1)

    files_to_delete.append(data_dir / '.gitignore')
    files_to_delete.append(data_dir / 'logfire_credentials.json')

    files_to_display = '\n'.join([str(file) for file in files_to_delete if file.exists()])
    confirm = input(f'The following files will be deleted:\n{files_to_display}\nAre you sure? [N/y]')
    if confirm.lower() in ('yes', 'y'):
        for file in files_to_delete:
            file.unlink(missing_ok=True)
        sys.stderr.write('Cleaned Logfire data.\n')
    else:
        sys.stderr.write('Clean aborted.\n')


def parse_inspect(args: argparse.Namespace) -> None:
    """Inspect installed packages and recommend packages that might be useful."""
    console = Console(file=sys.stderr)

    ctx = collect_instrumentation_context(set(args.ignore))

    if ctx.recommendations:
        print_otel_summary(console=console, recommendations=ctx.recommendations)
        sys.exit(1)
    else:
        console.print('No recommended packages found. You are all set!', style='green')  # pragma: no cover


def parse_list_projects(args: argparse.Namespace) -> None:
    """List user projects."""
    client = LogfireClient.from_url(args.logfire_url)

    projects = client.get_user_projects()
    if projects:
        sys.stderr.write("List of the projects you have write access to (requires the 'write_token' permission):\n\n")
        sys.stderr.write(
            _pretty_table(
                ['Organization', 'Project'],
                [
                    [project['organization_name'], project['project_name']]
                    for project in sorted(projects, key=itemgetter('organization_name', 'project_name'))
                ],
            )
        )
    else:
        sys.stderr.write(
            'No projects found for the current user. You can create a new project with `logfire projects new`\n'
        )


def _write_credentials(project_info: dict[str, Any], data_dir: Path, logfire_api_url: str) -> LogfireCredentials:
    try:
        credentials = LogfireCredentials(**project_info, logfire_api_url=logfire_api_url)
        credentials.write_creds_file(data_dir)
        return credentials
    except TypeError as e:
        raise LogfireConfigError(f'Invalid credentials, when initializing project: {e}') from e


def parse_create_new_project(args: argparse.Namespace) -> None:
    """Create a new project."""
    data_dir = Path(args.data_dir)
    client = LogfireClient.from_url(args.logfire_url)

    project_name = args.project_name
    organization = args.org
    default_organization = args.default_org
    project_info = LogfireCredentials.create_new_project(
        client=client,
        organization=organization,
        default_organization=default_organization,
        project_name=project_name,
    )
    credentials = _write_credentials(project_info, data_dir, client.base_url)
    sys.stderr.write(f'Project created successfully. You will be able to view it at: {credentials.project_url}\n')


def parse_create_read_token(args: argparse.Namespace) -> None:
    """Create a read token for a project."""
    client = LogfireClient.from_url(args.logfire_url)
    response = client.create_read_token(args.organization, args.project)
    sys.stdout.write(response['token'] + '\n')


def parse_use_project(args: argparse.Namespace) -> None:
    """Use an existing project."""
    data_dir = Path(args.data_dir)
    client = LogfireClient.from_url(args.logfire_url)

    project_name = args.project_name
    organization = args.org
    projects = client.get_user_projects()
    project_info = LogfireCredentials.use_existing_project(
        client=client,
        projects=projects,
        organization=organization,
        project_name=project_name,
    )
    if project_info:
        credentials = _write_credentials(project_info, data_dir, client.base_url)
        sys.stderr.write(
            f'Project configured successfully. You will be able to view it at: {credentials.project_url}\n'
        )


def logfire_info() -> str:
    """Show versions of logfire, OS and related packages."""
    import importlib.metadata as importlib_metadata

    # get data about packages that are closely related to logfire
    package_names = {
        # use by otel to send data
        'requests': 1,
        # custom integration
        'pydantic': 2,
        # otel integration is customed
        'fastapi': 3,
        # custom integration
        'openai': 4,
        # dependencies of otel
        'protobuf': 5,
        # dependencies
        'rich': 6,
        # dependencies
        'typing-extensions': 7,
        # dependencies
        'tomli': 8,
        # dependencies
        'executing': 9,
    }
    otel_index = max(package_names.values(), default=0) + 1
    related_packages: list[tuple[int, str, str]] = []

    for dist in importlib_metadata.distributions():
        metadata = dist.metadata
        name = metadata.get('Name', '')
        version = metadata.get('Version', 'UNKNOWN')
        index = package_names.get(name)
        if index is not None:
            related_packages.append((index, name, version))
        if name.startswith('opentelemetry'):
            related_packages.append((otel_index, name, version))

    toml_lines: tuple[str, ...] = (
        f'logfire="{VERSION}"',
        f'platform="{platform.platform()}"',
        f'python="{sys.version}"',
        '[related_packages]',
        *(f'{name}="{version}"' for _, name, version in sorted(related_packages)),
    )
    return '\n'.join(toml_lines) + '\n'


def parse_info(_args: argparse.Namespace) -> None:
    """Show versions of logfire, OS and related packages."""
    sys.stderr.writelines(logfire_info())


def _pretty_table(header: list[str], rows: list[list[str]]):
    rows = [[' ' + first, *rest] for first, *rest in [header] + rows]
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    lines = ['   | '.join(cell.ljust(width) for cell, width in zip(row, widths)) for row in rows]
    header_line = '---|-'.join('-' * width for width in widths)
    lines.insert(1, header_line)
    return '\n'.join(lines) + '\n'


def _get_logfire_url(logfire_url: str | None, region: str | None) -> str | None:
    if logfire_url is not None:
        return logfire_url
    if region is not None:
        return REGIONS[region]['base_url']


class SplitArgs(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,
    ):
        if isinstance(values, str):  # pragma: no branch
            values = values.split(',')
        namespace_value: list[str] = getattr(namespace, self.dest) or []
        setattr(namespace, self.dest, namespace_value + list(values or []))


class OrgProjectAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,
    ):
        if isinstance(values, str) and '/' in values:
            try:
                organization, project = values.split('/')
                if not organization or not project:
                    parser.error(f'Invalid format: {values}. Expected <org>/<project>')
                setattr(namespace, 'organization', organization)
                setattr(namespace, self.dest, project)
            except ValueError:
                parser.error(f'Invalid format: {values}. Expected <org>/<project>')
        else:
            parser.error(f'Invalid format: {values}. Expected <org>/<project>')


def _main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog='logfire',
        description='The CLI for Pydantic Logfire.',
        epilog='See https://logfire.pydantic.dev/docs/reference/cli/ for more detailed documentation.',
    )

    parser.add_argument('--version', action='store_true', help='show the version and exit')
    global_opts = parser.add_argument_group(title='global options')
    url_or_region_grp = global_opts.add_mutually_exclusive_group()
    url_or_region_grp.add_argument('--logfire-url', help=argparse.SUPPRESS)
    url_or_region_grp.add_argument(
        '--base-url', help='the base URL for self-hosted Logfire instances (e.g., http://localhost:8080)'
    )
    url_or_region_grp.add_argument('--region', choices=REGIONS, help='the region to use')
    parser.set_defaults(func=lambda _: parser.print_help())  # type: ignore
    subparsers = parser.add_subparsers(title='commands', metavar='')

    # NOTE(DavidM): Let's try to keep the commands listed in alphabetical order if we can
    cmd_auth = subparsers.add_parser('auth', help=parse_auth.__doc__.split('\n', 1)[0], description=parse_auth.__doc__)  # type: ignore
    cmd_auth.set_defaults(func=parse_auth)

    cmd_clean = subparsers.add_parser('clean', help=parse_clean.__doc__)
    cmd_clean.set_defaults(func=parse_clean)
    cmd_clean.add_argument('--data-dir', default='.logfire')
    cmd_clean.add_argument('--logs', action='store_true', default=False, help='remove the Logfire logs')

    cmd_inspect = subparsers.add_parser('inspect', help=parse_inspect.__doc__)
    cmd_inspect.set_defaults(func=parse_inspect)
    cmd_inspect.add_argument('--ignore', action=SplitArgs, default=(), help='ignore a package')

    cmd_whoami = subparsers.add_parser('whoami', help=parse_whoami.__doc__)
    cmd_whoami.set_defaults(func=parse_whoami)
    cmd_whoami.add_argument('--data-dir', default='.logfire')

    cmd_projects = subparsers.add_parser('projects', help='Project management for Logfire.')
    cmd_projects.set_defaults(func=lambda _: cmd_projects.print_help())  # type: ignore
    projects_subparsers = cmd_projects.add_subparsers()

    cmd_projects_list = projects_subparsers.add_parser('list', help='list projects')
    cmd_projects_list.set_defaults(func=parse_list_projects)

    cmd_projects_new = projects_subparsers.add_parser('new', help='create a new project')
    cmd_projects_new.add_argument('project_name', nargs='?', help='project name')
    cmd_projects_new.add_argument('--data-dir', default='.logfire')
    cmd_projects_new.add_argument('--org', help='project organization')
    cmd_projects_new.add_argument(
        '--default-org', action='store_true', help='whether to create project under user default organization'
    )
    cmd_projects_new.set_defaults(func=parse_create_new_project)

    cmd_projects_use = projects_subparsers.add_parser('use', help='use a project')
    cmd_projects_use.add_argument('project_name', nargs='?', help='project name')
    cmd_projects_use.add_argument('--org', help='project organization')
    cmd_projects_use.add_argument('--data-dir', default='.logfire')
    cmd_projects_use.set_defaults(func=parse_use_project)

    cmd_read_tokens = subparsers.add_parser('read-tokens', help='Manage read tokens for a project')
    cmd_read_tokens.add_argument('--project', action=OrgProjectAction, help='project in the format <org>/<project>')
    cmd_read_tokens.set_defaults(func=lambda _: cmd_read_tokens.print_help())  # type: ignore
    read_tokens_subparsers = cmd_read_tokens.add_subparsers()

    # With this command you can do:
    # claude mcp add logfire -e LOGFIRE_READ_TOKEN=$(logfire read-tokens --project kludex/potato create) -- uvx logfire-mcp@latest
    cmd_read_tokens_create = read_tokens_subparsers.add_parser('create', help=parse_create_read_token.__doc__)
    cmd_read_tokens_create.set_defaults(func=parse_create_read_token)

    cmd_prompt = subparsers.add_parser('prompt', help=parse_prompt.__doc__)
    agent_code_argument_group = cmd_prompt.add_argument_group(title='code agentic specific options')
    agent_code_group = agent_code_argument_group.add_mutually_exclusive_group()
    agent_code_group.add_argument('--claude', action='store_true', help='verify the Claude Code setup')
    agent_code_group.add_argument('--codex', action='store_true', help='verify the Cursor setup')
    agent_code_group.add_argument('--opencode', action='store_true', help='verify the OpenCode setup')
    cmd_prompt.add_argument('--project', action=OrgProjectAction, help='project in the format <org>/<project>')
    cmd_prompt.add_argument('issue', nargs='?', help='the issue to get a prompt for')
    cmd_prompt.set_defaults(func=parse_prompt)

    cmd_info = subparsers.add_parser('info', help=parse_info.__doc__)
    cmd_info.set_defaults(func=parse_info)

    cmd_run = subparsers.add_parser('run', help='Run Python scripts/modules with Logfire instrumentation')
    cmd_run.add_argument('--summary', action=argparse.BooleanOptionalAction, default=True, help='hide the summary box')
    cmd_run.add_argument('--exclude', action=SplitArgs, default=(), help='exclude a package from instrumentation')
    cmd_run.add_argument('-m', '--module', help='Run module as script')
    cmd_run.add_argument(
        'script_and_args', nargs=argparse.REMAINDER, help='Script path and arguments, or module arguments when using -m'
    )
    cmd_run.set_defaults(func=parse_run)

    # We first try to parse everything, and if it's not the `parse_run` command, we parse again to raise an error on
    # unknown args. This is to allow the `parse_run` command to forward unknown args to the script/module.
    namespace, unknown_args = parser.parse_known_args(args)
    if namespace.func == parse_run:
        namespace.script_and_args = unknown_args + (namespace.script_and_args or [])
    else:
        namespace = parser.parse_args(args)

    if namespace.logfire_url:
        warnings.warn(
            'The `--logfire-url` argument is deprecated. Use `--base-url` instead.',
            DeprecationWarning,
            stacklevel=2,
        )

    namespace.logfire_url = namespace.logfire_url or namespace.base_url
    namespace.logfire_url = _get_logfire_url(namespace.logfire_url, namespace.region)

    trace.set_tracer_provider(tracer_provider=SDKTracerProvider())
    tracer = trace.get_tracer(__name__)

    def log_trace_id(response: requests.Response, context: ContextCarrier, *args: Any, **kwargs: Any) -> None:
        logger.debug('context=%s url=%s', context, response.url)

    if namespace.version:
        version_callback()
    elif namespace.func in (parse_info, parse_run):
        namespace.func(namespace)
    else:
        with tracer.start_as_current_span('logfire._internal.cli'), requests.Session() as session:
            context = get_context()
            session.hooks = {'response': functools.partial(log_trace_id, context=context)}
            session.headers.update(context)
            namespace._session = session
            namespace.func(namespace)


def main(args: list[str] | None = None) -> None:
    """Run the CLI."""
    HOME_LOGFIRE.mkdir(exist_ok=True)

    file_handler = logging.FileHandler(LOGFIRE_LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    logging.basicConfig(handlers=[file_handler], level=logging.DEBUG)

    try:
        _main(args)
    except KeyboardInterrupt:
        sys.stderr.write('User cancelled.\n')
        sys.exit(1)
    finally:
        file_handler.close()
