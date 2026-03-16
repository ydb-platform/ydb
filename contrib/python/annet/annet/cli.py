import argparse
import itertools
import json
import operator
import os
import platform
import shutil
import subprocess
import sys
from contextlib import ExitStack, contextmanager
from typing import Iterable, Optional, Tuple

import tabulate
import yaml
from contextlog import get_logger
from valkit.python import valid_logging_level

from annet import api, cli_args, filtering, generators
from annet.api import Deployer, collapse_texts
from annet.argparse import ArgParser, subcommand
from annet.deploy import get_deployer, get_fetcher
from annet.diff import gen_sort_diff
from annet.gen import Loader, old_raw
from annet.lib import get_context_path, repair_context_file
from annet.output import OutputDriver, output_driver_connector
from annet.storage import Device, get_storage


def fill_base_args(parser: ArgParser, pkg_name: str, logging_config: str):
    parser.add_argument(
        "--log-level",
        default="WARN",
        type=valid_logging_level,
        help="Уровень детализации логов (DEBUG, DEBUG2 (with comocutor debug), INFO, WARN, CRITICAL)",
    )
    parser.add_argument("--pkg_name", default=pkg_name, help=argparse.SUPPRESS)
    parser.add_argument("--logging_config", default=logging_config, help=argparse.SUPPRESS)


def list_subcommands():
    return globals().copy()


def _gen_current_items(
    config,
    stdin,
    loader: Loader,
    output_driver: OutputDriver,
    gen_args: cli_args.GenOptions,
) -> Iterable[Tuple[str, str, bool]]:
    for device, result in old_raw(
        args=gen_args,
        loader=loader,
        config=config,
        stdin=stdin,
        do_files_download=True,
        use_mesh=False,
    ):
        if device.hw.vendor != "pc":
            destname = output_driver.cfg_file_names(device)[0]
            yield (destname, result, False)
        else:
            for entire_path, entire_data in sorted(result.items(), key=operator.itemgetter(0)):
                if entire_data is None:
                    entire_data = ""
                destname = output_driver.entire_config_dest_path(device, entire_path)
                yield (destname, entire_data, False)


@contextmanager
def get_loader(gen_args: cli_args.GenOptions, args: cli_args.QueryOptionsBase):
    exit_stack = ExitStack()
    storages = []
    with exit_stack:
        connector, conf_params = get_storage()
        storage_opts = connector.opts().parse_params(conf_params, args)
        storages.append(exit_stack.enter_context(connector.storage()(storage_opts)))
        yield Loader(*storages, args=gen_args, no_empty_warning=args.query.is_empty())


@subcommand(is_group=True)
def show():
    """A group of commands for showing parameters/configurations/data from deivces and data sources"""
    pass


@subcommand(cli_args.QueryOptions, cli_args.opt_config, cli_args.FileOutOptions, parent=show)
def show_current(args: cli_args.QueryOptions, config, arg_out: cli_args.FileOutOptions) -> None:
    """Show current devices' configuration"""
    gen_args = cli_args.GenOptions(args, no_acl=True)
    output_driver = output_driver_connector.get()
    with get_loader(gen_args, args) as loader:
        if not loader.devices:
            get_logger().error("No devices found for %s", args.query)

        items = _gen_current_items(
            loader=loader,
            output_driver=output_driver,
            gen_args=gen_args,
            stdin=args.stdin(config=config),
            config=config,
        )
        output_driver.write_output(arg_out, items, len(loader.devices))


@subcommand(cli_args.QueryOptions, cli_args.FileOutOptions, parent=show)
def show_device_dump(args: cli_args.QueryOptions, arg_out: cli_args.FileOutOptions):
    """Show a dump of network devices' structure"""

    def _show_device_dump_items(devices):
        for device in devices:
            get_logger(host=device.hostname)  # add hostname into context
            if hasattr(device, "dump"):
                yield (
                    device.hostname,
                    "\n".join(device.dump("device")),
                    False,
                )
            else:
                get_logger().warning("method `dump` not implemented for %s", type(device))

    arg_gens = cli_args.GenOptions(arg_out, args)
    with get_loader(arg_gens, args) as loader:
        if not loader.devices:
            get_logger().error("No devices found for %s", args.query)
        output_driver_connector.get().write_output(
            arg_out,
            _show_device_dump_items(loader.devices),
            len(loader.device_ids),
        )


@subcommand(cli_args.ShowGeneratorsOptions, parent=show)
def show_generators(args: cli_args.ShowGeneratorsOptions):
    """List applicable generators (for a device if query is set)"""
    arg_gens = cli_args.GenOptions(args)
    with get_loader(arg_gens, args) as loader:
        device: Optional[Device] = None
        devices = loader.devices
        if len(devices) == 1:
            device = devices[0]
        elif len(devices) > 1:
            get_logger().error("cannot show generators for more than one device at once")
            sys.exit(1)
        elif len(devices) == 0 and not args.query.is_empty():
            # the error message will be logged in get_loader()
            sys.exit(1)

        if not device:
            found_generators = loader.iter_all_gens()
        else:
            found_generators = []
            gens = loader.resolve_gens(loader.devices)
            for g in gens.partial[device]:
                acl_func = g.acl_safe if args.acl_safe else g.acl
                if g.supports_device(device) and acl_func(device):
                    found_generators.append(g)
            for g in gens.entire[device]:
                if g.supports_device(device) and g.path(device):
                    found_generators.append(g)
            for g in gens.json_fragment[device]:
                if g.supports_device(device) and g.path(device) and g.acl(device):
                    found_generators.append(g)

    output_data = []
    for g in found_generators:
        output_data.append(
            {
                "name": g.__class__.__name__,
                "type": g.TYPE,
                "tags": g.TAGS,
                "module": g.__class__.__module__,
                "description": generators.get_description(g.__class__),
            }
        )

    if args.format == "json":
        print(json.dumps(output_data))

    elif args.format == "text":
        keyfunc = operator.itemgetter("type")
        for gen_type, gens in itertools.groupby(sorted(output_data, key=keyfunc, reverse=True), keyfunc):
            print(
                tabulate.tabulate(
                    [(g["name"], ", ".join(g["tags"]), g["module"], g["description"]) for g in gens],
                    [f"{gen_type}-Class", "Tags", "Module", "Description"],
                    tablefmt="orgtbl",
                )
            )
            print()


@subcommand(cli_args.ShowGenOptions)
def gen(args: cli_args.ShowGenOptions):
    """Generate configuration for devices"""
    with get_loader(args, args) as loader:
        (success, fail) = api.gen(args, loader)

        out = [item for items in success.values() for item in items]
        output_driver = output_driver_connector.get()
        if args.dest is None:
            text_mapping = {item[0]: item[1] for item in out}
            out = [(", ".join(key), value, False) for key, value in collapse_texts(text_mapping).items()]

        out.extend(output_driver.format_fails(fail, loader.device_fqdns))
        total = len(success) + len(fail)
        if not total:
            get_logger().error("No devices found for %s", args.query)
        output_driver.write_output(args, out, total)


@subcommand(cli_args.ShowDiffOptions)
def diff(args: cli_args.ShowDiffOptions):
    """Generate configuration for devices and show a diff with current configuration using the rulebook"""
    with get_loader(args, args) as loader:
        success, fail = api.diff(args, loader, loader.device_ids)

        out = list(gen_sort_diff({loader.get_device(k): v for k, v in success.items()}, args))
        output_driver = output_driver_connector.get()
        if args.dest is None:
            text_mapping = {item[0]: item[1] for item in out}
            out = [(", ".join(key), value, False) for key, value in collapse_texts(text_mapping).items()]
        out.extend(output_driver.format_fails(fail, loader.device_fqdns))

        if total := len(success) + len(fail):
            output_driver.write_output(args, out, total)
        else:
            get_logger().error("No devices found for %s", args.query)


@subcommand(cli_args.ShowPatchOptions)
def patch(args: cli_args.ShowPatchOptions):
    """Generate configuration patch for devices"""
    with get_loader(args, args) as loader:
        (success, fail) = api.patch(args, loader)

        out = [item for items in success.values() for item in items]
        output_driver = output_driver_connector.get()
        out.extend(output_driver.format_fails(fail, loader.device_fqdns))
        total = len(success) + len(fail)
        if not total:
            get_logger().error("No devices found for %s", args.query)
        output_driver.write_output(args, out, total)


@subcommand(cli_args.DeployOptions)
def deploy(args: cli_args.DeployOptions):
    """Generate and deploy configuration for devices"""

    deployer = Deployer(args)
    filterer = filtering.filterer_connector.get()
    fetcher = get_fetcher()
    deploy_driver = get_deployer()

    with get_loader(args, args) as loader:
        return api.deploy(
            args=args,
            loader=loader,
            deployer=deployer,
            deploy_driver=deploy_driver,
            filterer=filterer,
            fetcher=fetcher,
        )


@subcommand(cli_args.FileDiffOptions)
def file_diff(args: cli_args.FileDiffOptions):
    """Generate a diff between files or directories using the rulebook"""
    (success, fail) = api.file_diff(args)
    out = []
    output_driver = output_driver_connector.get()
    if not args.fails_only:
        out.extend(item for items in success.values() for item in items)
    out.extend(output_driver.format_fails(fail))
    # todo отрефакторить логику с отображением хоста в диффе: передавать в write_output явно критерий
    output_driver.write_output(args, out, len(out) + 1)


@subcommand(cli_args.FilePatchOptions)
def file_patch(args: cli_args.FilePatchOptions):
    """Generate configuration patch for files or directories"""
    (success, fail) = api.file_patch(args)
    out = []
    output_driver = output_driver_connector.get()
    if not args.fails_only:
        out.extend(item for items in success.values() for item in items)
    out.extend(output_driver.format_fails(fail))
    output_driver.write_output(args, out, len(out))


@subcommand(is_group=True)
def context():
    """A group of commands for manipulating context.

    By default, the context file is located in '~/.annet/context.yml',
    but it can be set with the ANN_CONTEXT_CONFIG_PATH environment variable.
    """
    context_touch()


@subcommand(parent=context)
def context_touch():
    """Show the context file path, and if the file is not present, create it with the default configuration"""
    print(get_context_path(touch=True))


@subcommand(cli_args.SelectContext, parent=context)
def context_set_context(args: cli_args.SelectContext):
    """Set the current active context.

    The selected context is used by default unless the environment variable ANN_SELECTED_CONTEXT is set
    """
    with open(path := get_context_path(touch=True)) as f:
        data = yaml.safe_load(f)
    if args.context_name not in data.get("context", {}):
        raise KeyError(
            f"Cannot select context with name '{args.context_name}'. "
            f"Available options are: {list(data.get('context', []))}"
        )
    data["selected_context"] = args.context_name
    with open(path, "w") as f:
        yaml.dump(data, f, sort_keys=False)


@subcommand(parent=context)
def context_edit():
    """Open the context file using an editor from the EDITOR environment variable.

    If the EDITOR variable is not set, default variables are: "notepad.exe" for Windows and "vi" otherwise
    """
    if e := os.getenv("EDITOR"):
        editor = e
    elif platform.system() == "Windows":
        editor = "notepad.exe"
    elif shutil.which("vim"):
        editor = "vim"
    else:
        editor = "vi"
    path = get_context_path(touch=True)
    proc = subprocess.Popen([editor, path])
    proc.wait()


@subcommand(parent=context)
def context_repair():
    """Try to fix the context file's structure if it was generated for the older versions of annet"""
    repair_context_file()
