# pylint: disable=too-many-ancestors

import abc
import argparse
import enum
import logging
import os

from valkit.common import valid_string_list

import annet.lib
from annet.argparse import Arg, ArgGroup, DefaultFromEnv
from annet.hardware import hardware_connector
from annet.storage import Query, get_storage


# ====
def valid_vendor(vendor):
    hw_provider = hardware_connector.get()
    hw = hw_provider.vendor_to_hw(vendor)
    if hw:
        return hw.vendor
    return ""


def convert_to_none(arg):
    return None if arg == "-" else arg


def valid_config_source(value):
    if value not in ["running", "empty", "-"] and not os.path.exists(value):
        raise ValueError("No such file or directory %r" % value)
    return value


def valid_range(value: str):
    if value.isdigit():
        return slice(0, int(value))
    elif ":" in value:
        start_str, stop_str = value.split(":", 1)
        if not stop_str:
            stop = None
        else:
            stop = int(stop_str)
        return slice(int(start_str), stop)

    raise ValueError("Invalid range: %s" % value)


def opt_query_factory(**kwargs):
    return Arg(
        "query",
        help="A query that defines a list of devices. The query's format depends on a selected storage adapter",
        **kwargs,
    )


# ====
opt_query = opt_query_factory(nargs="+")

opt_query_optional = opt_query_factory(nargs="*", default=[])

opt_dest = Arg("--dest", type=convert_to_none, help="A file or a directory to output the generated data to")

opt_expand_path = Arg(
    "--expand-path",
    default=False,
    help="Use full paths of Entire-generators and no just names when writing them to the file system",
)

opt_dest_force_create_dir = Arg(
    "--dest-force-create-dir", default=False, help="Output generated data to dir, even for blackboxes"
)

opt_old = Arg("old", help="A path to a file (or a directory with a batch of files) that contains the old config")

opt_new = Arg("new", help="A path to a file (or a directory with a batch of files) that contains the new config")

opt_hw = Arg(
    "--hw",
    default="",
    type=valid_vendor,
    help="Device's vendor (Huawei/Cisco/...) or model name. If left empty, annet will try to detect it automatically",
)

opt_indent = Arg("--indent", default="  ", help="Symbols to indent blocks with")

opt_allowed_gens = Arg(
    "-g", "--allowed-gens", type=valid_string_list, help="Comma-separated list of generator classes or tags to run"
)

opt_excluded_gens = Arg(
    "-G", "--excluded-gens", type=valid_string_list, help="Comma-separated list of generator classes or tags to skip"
)

opt_force_enabled = Arg(
    "--force-enabled",
    type=valid_string_list,
    help="Comma-separated list of generator classes or tags, for which the DISABLED tag should be ignored",
)

opt_generators_context = Arg("--generators_context", type=str, default=None, help=argparse.SUPPRESS)

opt_no_acl = Arg("--no-acl", default=False, help="Disable ACL for config generation")

opt_no_acl_exclusive = Arg(
    "--no-acl-exclusive", default=False, help="Check that ACLs of the executed generators do not intersect"
)

opt_acl_safe = Arg("--acl-safe", default=False, help="Use stricter safe acl for filtering generation results")

opt_show_rules = Arg("--show-rules", default=False, help="Show rulebook rules when showing the diff")

opt_tolerate_fails = Arg("--tolerate-fails", default=False, help="Show errors without interrupting the generation")

opt_recache = Arg("--recache", default=False, help="Force expiration of storage's local cache if it is supported")


# При параллельном запуске и включённом --tolerate-fails код возврата
# всегда нулевой. Это не позволяет нам легко понять, прошла ли генерация
# успешно для всех устройств. С этим флажком код будет ненулевой, если
# генерация упала хотя бы для одного устройства. А в хелпе эту переменную
# не выводим, там и так не протолкнуться от флагов.
opt_strict_exit_code = Arg(
    "--strict-exit-code",
    default=False,
    help=argparse.SUPPRESS,
)

opt_required_packages_check = Arg(
    "--required-packages-check",
    default=False,
    help="Check that the deb-packages, required for the Entire-generators, are installed",
)

opt_profile = Arg("--profile", default=False, help="Print time spent by generators and inventory requests to stderr")


opt_parallel = Arg(
    "-P",
    "--parallel",
    type=int,
    default=1,
    help="An amount of threads/processes to use as defined in a configured connector",
)

opt_max_tasks = Arg(
    "--max-tasks",
    type=int,
    default=None,
    help="Maximum consecutive tasks to run in a worker before restarting it to clear memory and cache. "
    "Defaults to infinity",
)

opt_annotate = Arg("--annotate", default=False, help="Annotate configuration lines to show their sources")

opt_config = Arg(
    "--config",
    default="running",
    type=valid_config_source,
    help="'running', 'empty', path to a config file or a directory with config files <hostname>.cfg or '-' (stdin)",
)

opt_clear = Arg("--clear", default=False, help="Remove the generator's commands using its ACL ")

opt_filter_acl = Arg("--filter-acl", default="", help="Additional ACL file path or '-' (stdin)")

opt_filter_ifaces = Arg(
    "-i",
    "--filter-ifaces",
    default=[],
    type=valid_string_list,
    help="Additional filter-acl using interface name. "
    "Accepts comma-separated regular expressions: '-i 10GE,100GE'. "
    "Appends '.*' at the end of every item "
    "One can use '$' for an exact match: '-i ae0$'. "
    "If filter-acl is passed directly, this option is ignored.",
)

opt_filter_peers = Arg(
    "-fp",
    "--filter-peers",
    default=[],
    type=valid_string_list,
    help="Generates filter-acl using an address or name of a peer's group or description.",
)

opt_filter_policies = Arg(
    "-frp",
    "--filter-policies",
    default=[],
    type=valid_string_list,
    help="Generates filter-acl using policies' names. The match has to be exact",
)

opt_ask_pass = Arg("--ask-pass", default=False, help="Ask for a password when connecting")

opt_no_ask_deploy = Arg("--no-ask-deploy", default=False, help="Do not ask for confirmation on commands execution")

opt_no_progress = Arg("--no-progress", default=False, help="Do not show deployment progress bars")

opt_log_json = Arg("--log-json", default=False, help="Show logs in json format (default: plain text)")

opt_log_dest = Arg(
    "--log-dest", default=annet.lib.get_default_log_dest, help="Log to a specified file, directory, or '-' (stdout)"
)

opt_log_nogroup = Arg(
    "--log-nogroup", default=False, help="Do not create DATE_TIME/ subdirectories in the LOG-DEST directory"
)

opt_max_slots = Arg(
    "--max-slots", default=30, type=int, help="The amount of devices parsed at the same time with asyncio"
)

opt_max_deploy = Arg(
    "--max-deploy",
    default=DefaultFromEnv("ANN_MAX_DEPLOY", "0"),
    type=int,
    help="The amount of devices deployed at the same time",
)

opt_hosts_range = Arg(
    "--hosts-range",
    type=valid_range,
    help="Only work with the specified hosts range: 10 - the first 10. 10:20 - host from 10th up to 20th",
)

opt_add_comments = Arg(
    "--add-comments",
    default=False,
    help=argparse.SUPPRESS,
)

opt_no_label = Arg("--no-label", default=False, help="Hide the file name label from the output")

opt_no_color = Arg("--no-color", default=False, help="Do not use the output ANSI-coloring (turns on when using --dest)")

opt_no_check_diff = Arg("--no-check-diff", default=False, help="Don't check diffs after deploying")

opt_dont_commit = Arg("--dont-commit", default=False, help="Do not run the 'commit' command during a deploy")

opt_rollback = Arg("--rollback", default=False, help="Suggest a rollback after a deployment if possible")

opt_fail_on_empty_config = Arg(
    "--fail-on-empty-config",
    default=False,
    help=argparse.SUPPRESS,
)


opt_show_generators_format = Arg("--format", default="text", choices=["text", "json"], help="Output format")


class EntireReloadFlag(enum.Enum):
    no = "no"
    yes = "yes"
    force = "force"

    def __bool__(self):
        return self is not self.no

    def __str__(self):
        return str(self.value)

    __repr__ = __str__


opt_entire_reload = Arg(
    "--entire-reload",
    type=EntireReloadFlag,
    default=EntireReloadFlag.yes,
    choices=list(EntireReloadFlag),
    const=EntireReloadFlag.yes,
    nargs="?",
    help="Run reload() when deploying the Entire-generators. no/yes/force - no/if the file was changed/always",
)

opt_show_hosts_progress = Arg("--show-hosts-progress", default=False, help="Show the progress percentage per host")

opt_no_collapse = Arg(
    "--no-collapse",
    default=False,
    help="Do not collapse identical diffs for a device group (turns on when using --dest)",
)

opt_fails_only = Arg("--fails-only", default=False, help="Показать только устройства с ошибками")

opt_include_missing = Arg("--include-missing", default=False, help="Include files that exist in only one directory")

opt_connect_timeout = Arg(
    "--connect-timeout",
    default=DefaultFromEnv("ANN_CONNECT_TIMEOUT", "20.0"),
    type=float,
    help="Timeout for connecting to a device, in seconds."
    " The default value can be set in the ANN_CONNECT_TIMEOUT environment variable.",
)

opt_selected_context_name = Arg("context-name", type=str, help="Name of a context from the context file to use")


# ====
class CacheOptions(ArgGroup):
    recache = opt_recache
    no_mesh = False


class QueryOptionsBase(CacheOptions):
    @property
    @abc.abstractmethod
    def query(self) -> "Query":
        pass

    @property
    @abc.abstractmethod
    def hosts_range(self):
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not isinstance(self.query, Query):
            storage, _ = get_storage()
            query_type = storage.query()
            self.query = query_type.new(self.query, hosts_range=self.hosts_range)


class QueryOptions(QueryOptionsBase):
    query = opt_query
    hosts_range = opt_hosts_range


class QueryOptionsOptional(QueryOptionsBase):
    query = opt_query_optional
    hosts_range = opt_hosts_range


class ParallelOptions(ArgGroup):
    parallel = opt_parallel
    max_tasks = opt_max_tasks


class GenSelectOptions(ArgGroup):
    allowed_gens = opt_allowed_gens
    excluded_gens = opt_excluded_gens
    force_enabled = opt_force_enabled
    generators_context = opt_generators_context
    ignore_disabled = False


class GenOptions(QueryOptions, GenSelectOptions, CacheOptions, ParallelOptions):
    no_acl = opt_no_acl
    no_acl_exclusive = opt_no_acl_exclusive
    acl_safe = opt_acl_safe
    filter_acl = opt_filter_acl
    filter_ifaces = opt_filter_ifaces
    filter_peers = opt_filter_peers
    filter_policies = opt_filter_policies
    profile = opt_profile
    tolerate_fails = opt_tolerate_fails
    required_packages_check = opt_required_packages_check
    strict_exit_code = opt_strict_exit_code
    fail_on_empty_config = opt_fail_on_empty_config


class ComocutorOptions(ArgGroup):
    ask_pass = opt_ask_pass
    max_slots = opt_max_slots
    no_progress = opt_no_progress
    connect_timeout = opt_connect_timeout


class CliLoggingOptions(ArgGroup):
    log_json = opt_log_json
    log_dest = opt_log_dest
    log_nogroup = opt_log_nogroup


class DeviceCliOptions(ComocutorOptions, CliLoggingOptions):
    no_ask_deploy = opt_no_ask_deploy
    dont_commit = opt_dont_commit


class FileOutOptions(ArgGroup):
    dest = opt_dest
    expand_path = opt_expand_path
    no_label = opt_no_label
    no_color = opt_no_color
    dest_force_create_dir = opt_dest_force_create_dir

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.dest:
            self.no_color = True


class DiffOptions(GenOptions, ComocutorOptions):
    clear = opt_clear
    config = opt_config
    show_hosts_progress = opt_show_hosts_progress


class FileInputOptions(ArgGroup):
    old = opt_old
    new = opt_new
    hw = opt_hw
    fails_only = opt_fails_only
    include_missing = opt_include_missing


class PatchOptions(DiffOptions):
    add_comments = opt_add_comments


class ShowGenOptions(GenOptions, FileOutOptions):
    indent = opt_indent
    annotate = opt_annotate
    show_hosts_progress = opt_show_hosts_progress


class ShowDiffOptions(DiffOptions, FileOutOptions):
    indent = opt_indent
    show_rules = opt_show_rules
    no_collapse = opt_no_collapse

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.dest:
            self.no_collapse = True


class ShowPatchOptions(PatchOptions, FileOutOptions):
    indent = opt_indent
    show_hosts_progress = opt_show_hosts_progress


class FileDiffOptions(FileInputOptions, FileOutOptions, ParallelOptions):
    indent = opt_indent
    show_rules = opt_show_rules


class FilePatchOptions(FileInputOptions, FileOutOptions, ParallelOptions):
    indent = opt_indent
    add_comments = opt_add_comments


class ShowGeneratorsOptions(QueryOptionsOptional, GenSelectOptions):
    format = opt_show_generators_format
    acl_safe = opt_acl_safe
    ignore_disabled = False


class DeployOptions(ShowDiffOptions, PatchOptions, DeviceCliOptions):
    no_check_diff = opt_no_check_diff
    entire_reload = opt_entire_reload
    rollback = opt_rollback
    max_parallel = opt_max_deploy


class SelectContext(ArgGroup):
    context_name = opt_selected_context_name
