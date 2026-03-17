import abc
import inspect
import os
import re
import sys
import time
import warnings
from collections import OrderedDict as odict
from itertools import chain, groupby
from operator import itemgetter
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Set,
    Tuple,
    Union,
    cast,
)

import colorama
from contextlog import get_logger

import annet.deploy
import annet.deploy_ui
import annet.lib
from annet import cli_args, filtering, patching, rulebook, tracing
from annet import diff as ann_diff
from annet import gen as ann_gen
from annet.annlib import jsontools
from annet.annlib.netdev.views.hardware import HardwareView
from annet.annlib.types import GeneratorType
from annet.deploy import DeployDriver, Fetcher
from annet.diff import file_differ_connector
from annet.filtering import Filterer
from annet.hardware import hardware_connector
from annet.output import format_file_diff, output_driver_connector, print_err_label
from annet.parallel import Parallel, TaskResult
from annet.reference import RefTracker
from annet.rulebook import deploying
from annet.storage import Device, get_storage
from annet.types import Diff, ExitCode, OldNewResult, Op, PCDiff
from annet.vendors import registry_connector, tabparser


DEFAULT_INDENT = "  "


def patch_from_pre(pre, hw, rb, add_comments, ref_track=None, do_commit=True):
    if not ref_track:
        ref_track = RefTracker()
    orderer = patching.Orderer(rb["ordering"], hw.vendor)
    orderer.ref_insert(ref_track)
    return patching.make_patch(
        pre=pre,
        rb=rb,
        hw=hw,
        add_comments=add_comments,
        orderer=orderer,
        do_commit=do_commit,
    )


def _diff_and_patch(
    device, old, new, acl_rules, filter_acl_rules, add_comments, ref_track=None, do_commit=True, rb=None
) -> Tuple[Diff, Dict]:
    if rb is None:
        rb = rulebook.get_rulebook(device.hw)
    # [NOCDEV-5532] Передаем в diff только релевантные для logic'a части конфига
    if acl_rules is not None:
        old = patching.apply_acl(old, acl_rules)
        new = patching.apply_acl(new, acl_rules, with_annotations=add_comments)

    diff_tree = patching.make_diff(old, new, rb, [acl_rules, filter_acl_rules])
    pre = patching.make_pre(diff_tree)
    patch_tree = patch_from_pre(pre, device.hw, rb, add_comments, ref_track, do_commit)
    diff_tree = patching.strip_unchanged(diff_tree)

    return (diff_tree, patch_tree)


# =====
def _read_old_new_diff_patch(old: Dict[str, Dict], new: Dict[str, Dict], hw: HardwareView, add_comments: bool):
    rb = rulebook.get_rulebook(hw)
    diff_obj = patching.make_diff(old, new, rb, [])
    diff_obj = patching.strip_unchanged(diff_obj)
    pre = patching.make_pre(diff_obj)
    patchtree = patch_from_pre(pre, hw, rb, add_comments)
    return rb, diff_obj, pre, patchtree


def _read_old_new_configs(old_path: str, new_path: str, empty_missing: bool = False) -> tuple[str, str]:
    _logger = get_logger()
    ret = []
    for path in (old_path, new_path):
        _logger.debug("Reading %r ...", path)
        if not os.path.exists(path):
            if empty_missing:
                ret.append("")
                continue
            else:
                raise FileNotFoundError(f"File {path} not found")
        with open(path) as f:
            ret.append(f.read())

    return tuple(ret)


def _read_old_new_hw(old_path: str, old_config: str, new_path: str, new_config: str, args: cli_args.FileInputOptions):
    _logger = get_logger()

    hw = args.hw
    if isinstance(args.hw, str):
        hw = HardwareView(args.hw, "")

    try:
        old, old_hw, old_score = _parse_device_config(old_config, hw)
    except tabparser.ParserError:
        _logger.exception("Parser error: %r", old_path)
        raise

    try:
        new, new_hw, new_score = _parse_device_config(new_config, hw)
    except tabparser.ParserError:
        _logger.exception("Parser error: %r", new_path)
        raise

    hw = new_hw
    if old_score > new_score:
        hw = old_hw
    if old_hw != new_hw:
        _logger.warning("Old and new detected hw differs, assume %r", hw)

    return old, new, hw


@tracing.function
def _read_old_new_cfgdumps(args: cli_args.FileInputOptions):
    _logger = get_logger()
    old_path, new_path = os.path.normpath(args.old), os.path.normpath(args.new)
    if not os.path.isdir(old_path):
        yield (old_path, new_path)
        return
    _logger.info("Scanning cfgdumps: %s/*.cfg ...", old_path)
    cfgdump_reg = re.compile(r"^[^\s]+\.cfg$")
    if os.path.isdir(old_path) and os.path.isdir(new_path):
        if cfgdump_reg.match(os.path.basename(old_path)) and cfgdump_reg.match(os.path.basename(new_path)):
            yield (old_path, new_path)
    names = sorted(set(os.listdir(old_path)) | set(os.listdir(new_path)))
    for name in names:
        old_path_name = os.path.join(old_path, name)
        new_path_name = os.path.join(new_path, name)

        if not args.include_missing:
            skipped = False
            for path in (old_path_name, new_path_name):
                if not os.path.exists(path):
                    _logger.debug("Ignoring file %s: not exist %s", name, path)
                    skipped = True
                    break
            if skipped:
                continue

        yield (old_path_name, new_path_name)


def _parse_device_config(text, hw):
    score = 1
    vendor_registry = registry_connector.get()

    if not hw:
        hw, score = guess_hw(text)

    config = tabparser.parse_to_tree(
        text=text,
        splitter=vendor_registry.match(hw).make_formatter().split,
    )

    return config, hw, score


# =====
def _format_patch_blocks(patch_tree, hw, indent):
    formatter = registry_connector.get().match(hw).make_formatter(indent=indent)
    return formatter.patch(patch_tree)


# =====
def _print_pre_as_diff(pre, show_rules, indent, file=None, _level=0):
    for raw_rule, content in sorted(pre.items(), key=itemgetter(0)):
        rule_printed = False
        for op, sign in [  # FIXME: Not very effective
            (Op.REMOVED, colorama.Fore.RED + "-"),
            (Op.ADDED, colorama.Fore.GREEN + "+"),
            (Op.AFFECTED, colorama.Fore.CYAN + " "),
        ]:
            items = content["items"].items()
            for _, diff in items:  # pylint: disable=redefined-outer-name
                if show_rules and not rule_printed and not raw_rule == "__MULTILINE_BODY__":
                    print(
                        "%s%s# %s%s%s"
                        % (
                            colorama.Style.BRIGHT,
                            colorama.Fore.BLACK,
                            (indent * _level),
                            raw_rule,
                            colorama.Style.RESET_ALL,
                        ),
                        file=file,
                    )
                    rule_printed = True
                for item in sorted(diff[op], key=itemgetter("row")):
                    print(
                        "%s%s%s %s%s"
                        % (colorama.Style.BRIGHT, sign, (indent * _level), item["row"], colorama.Style.RESET_ALL),
                        file=file,
                    )
                    if len(item["children"]) != 0:
                        _print_pre_as_diff(item["children"], show_rules, indent, file, _level + 1)
                        rule_printed = False


class PoolProgressLogger:
    def __init__(self, device_fqdns: Dict[int, str]):
        self.device_fqdns = device_fqdns

    def __call__(self, pool: Parallel, task_result: TaskResult):
        progress_logger = get_logger("progress")
        perc = int(pool.tasks_done / len(self.device_fqdns) * 100)

        fqdn = self.device_fqdns[task_result.device_id]
        elapsed_time = "%dsec" % int(time.monotonic() - task_result.extra["start_time"])
        if task_result.extra.get("regression", False):
            status = task_result.extra["status"]
            status_color = task_result.extra["status_color"]
            message = task_result.extra["message"]
        else:
            status = "OK" if task_result.exc is None else "FAIL"
            status_color = colorama.Fore.GREEN if status == "OK" else colorama.Fore.RED
            message = "" if status == "OK" else str(task_result.exc)
        progress_logger.info(
            message,
            perc=perc,
            fqdn=fqdn,
            status=status,
            status_color=status_color,
            worker=task_result.worker_name,
            task_time=elapsed_time,
        )
        return task_result


def log_host_progress_cb(pool: Parallel, task_result: TaskResult):
    warnings.warn(
        "log_host_progress_cb is deprecated, use PoolProgressLogger",
        DeprecationWarning,
        stacklevel=2,
    )
    args = cast(cli_args.QueryOptions, pool.args[0])
    connector = get_storage()
    storage_opts = connector.opts().parse_params({}, args)
    with connector.storage()(storage_opts) as storage:
        fqdns = storage.resolve_fdnds_by_query(args.query)
    PoolProgressLogger(device_fqdns=fqdns)(pool, task_result)


# =====
def gen(args: cli_args.ShowGenOptions, loader: ann_gen.Loader):
    """Сгенерировать конфиг для устройств"""
    stdin = args.stdin(filter_acl=args.filter_acl, config=None)

    filterer = filtering.filterer_connector.get()
    pool = Parallel(ann_gen.worker, args, stdin, loader, filterer).tune_args(args)
    if args.show_hosts_progress:
        pool.add_callback(PoolProgressLogger(loader.device_fqdns))

    return pool.run(loader.device_ids, args.tolerate_fails, args.strict_exit_code)


# =====


def patch(args: cli_args.ShowPatchOptions, loader: ann_gen.Loader):
    """Сгенерировать патч для устройств"""
    if args.config == "running":
        fetcher = annet.deploy.get_fetcher()
        ann_gen.live_configs = annet.lib.do_async(fetcher.fetch(loader.devices, processes=args.parallel))
    stdin = args.stdin(filter_acl=args.filter_acl, config=args.config)

    filterer = filtering.filterer_connector.get()
    pool = Parallel(_patch_worker, args, stdin, loader, filterer).tune_args(args)
    if args.show_hosts_progress:
        pool.add_callback(PoolProgressLogger(loader.device_fqdns))
    return pool.run(loader.device_ids, args.tolerate_fails, args.strict_exit_code)


def _patch_worker(
    device_id, args: cli_args.ShowPatchOptions, stdin, loader: ann_gen.Loader, filterer: filtering.Filterer
):
    for res, _, patch_tree in res_diff_patch(device_id, args, stdin, loader, filterer):
        old_files = res.old_files
        new_files = res.get_new_files(args.acl_safe)
        new_json_fragment_files = res.get_new_file_fragments(args.acl_safe)
        if new_files:
            for path, (cfg_text, _cmds) in new_files.items():
                label = res.device.hostname + os.sep + path
                if old_files.get(path) != cfg_text:
                    yield label, cfg_text, False
        elif res.old_json_fragment_files or new_json_fragment_files:
            for path, (new_json_cfg, _cmds) in new_json_fragment_files.items():
                label = res.device.hostname + os.sep + path
                old_json_cfg = res.old_json_fragment_files[path]
                json_patch = jsontools.make_patch(old_json_cfg, new_json_cfg)
                yield (
                    label,
                    jsontools.format_json(json_patch),
                    False,
                )
        elif patch_tree:
            yield (
                "%s.patch" % res.device.hostname,
                _format_patch_blocks(patch_tree, res.device.hw, args.indent),
                False,
            )


# =====
def res_diff_patch(
    device_id,
    args: cli_args.ShowPatchOptions,
    stdin,
    loader: ann_gen.Loader,
    filterer: filtering.Filterer,
) -> Iterable[Tuple[OldNewResult, Dict, Dict]]:
    for res in ann_gen.old_new(
        args,
        config=args.config,
        loader=loader,
        filterer=filterer,
        stdin=stdin,
        device_ids=[device_id],
        no_new=args.clear,
        do_files_download=True,
    ):
        old = res.get_old(args.acl_safe)
        new = res.get_new(args.acl_safe)
        new_json_fragment_files = res.get_new_file_fragments(args.acl_safe)

        device = res.device
        acl_rules = res.get_acl_rules(args.acl_safe)
        if res.old_json_fragment_files or new_json_fragment_files:
            yield res, None, None
        elif old is not None:
            (diff_tree, patch_tree) = _diff_and_patch(
                device, old, new, acl_rules, res.filter_acl_rules, args.add_comments
            )
            yield res, diff_tree, patch_tree


def diff(
    args: cli_args.DiffOptions, loader: ann_gen.Loader, device_ids: List[Any]
) -> tuple[Mapping[Device, Union[Diff, PCDiff]], Mapping[Device, Exception]]:
    """Сгенерировать дифф для устройств"""
    if args.config == "running":
        fetcher = annet.deploy.get_fetcher()
        ann_gen.live_configs = annet.lib.do_async(
            fetcher.fetch([device for device in loader.devices if device.id in device_ids], processes=args.parallel),
            new_thread=True,
        )
    stdin = args.stdin(filter_acl=args.filter_acl, config=None)

    filterer = filtering.filterer_connector.get()
    pool = Parallel(ann_diff.worker, args, stdin, loader, filterer).tune_args(args)
    if args.show_hosts_progress:
        fqdns = {k: v for k, v in loader.device_fqdns.items() if k in device_ids}
        pool.add_callback(PoolProgressLogger(fqdns))

    return pool.run(device_ids, args.tolerate_fails, args.strict_exit_code)


def collapse_texts(texts: Mapping[str, str | Generator[str, None, None]]) -> Mapping[Tuple[str, ...], str]:
    """
    Группировка текстов.
    :param texts:
    :return: словарь с несколькими хостнеймами в ключе.
    """
    diffs_with_orig = {}
    for key, value in texts.items():
        if inspect.isgenerator(value):
            lines = list(value)
            diffs_with_orig[key] = ["".join(lines), lines]
        else:
            diffs_with_orig[key] = [value, value.splitlines()]

    res = {}
    for _, collapsed_diff_iter in groupby(
        sorted(diffs_with_orig.items(), key=lambda x: (x[0], x[1][1])), key=lambda x: x[1][1]
    ):
        collapsed_diff = list(collapsed_diff_iter)
        res[tuple(x[0] for x in collapsed_diff)] = collapsed_diff[0][1][0]

    return res


class DeployerJob(abc.ABC):
    def __init__(self, device, args: cli_args.DeployOptions):
        self.args = args
        self.device = device
        self.add_comments = False
        self.diff_lines = []
        self.cmd_lines: List[str] = []
        self.deploy_cmds = odict()
        self.diffs = {}
        self.failed_configs = {}
        self._has_diff = False

    @abc.abstractmethod
    def parse_result(self, res):
        pass

    def collapseable_diffs(self):
        return {}

    def has_diff(self):
        return self._has_diff

    @staticmethod
    def from_device(device, args: cli_args.DeployOptions):
        if device.hw.vendor == "pc":
            return PCDeployerJob(device, args)
        return CliDeployerJob(device, args)


class CliDeployerJob(DeployerJob):
    def parse_result(self, res: OldNewResult):
        device = res.device
        old = res.get_old(self.args.acl_safe)
        new = res.get_new(self.args.acl_safe)
        acl_rules = res.get_acl_rules(self.args.acl_safe)
        err = res.err

        if err:
            self.failed_configs[device.fqdn] = err
            return

        (diff_obj, patch_tree) = _diff_and_patch(
            device, old, new, acl_rules, res.filter_acl_rules, self.add_comments, do_commit=not self.args.dont_commit
        )

        formatter = registry_connector.get().match(device.hw).make_formatter(indent="")
        cmds = formatter.cmd_paths(patch_tree)
        if not cmds:
            return

        self._has_diff = True
        self.diffs[device] = diff_obj
        self.cmd_lines.extend(["= %s " % device.hostname, ""])
        self.cmd_lines.extend(map(itemgetter(-1), cmds))
        self.cmd_lines.append("")
        deployer_driver = annet.deploy.get_deployer()
        self.deploy_cmds[device] = deployer_driver.apply_deploy_rulebook(
            device.hw, cmds, do_commit=not self.args.dont_commit
        )
        for cmd in deployer_driver.build_exit_cmdlist(device.hw):
            self.deploy_cmds[device].add_cmd(cmd)

    def collapseable_diffs(self):
        return self.diffs


class PCDeployerJob(DeployerJob):
    def parse_result(self, res: ann_gen.OldNewResult):
        device = res.device
        old_files = res.old_files
        new_files = res.get_new_files(self.args.acl_safe)
        old_json_fragment_files = res.old_json_fragment_files
        new_json_fragment_files = res.get_new_file_fragments(self.args.acl_safe)
        err = res.err

        if err:
            self.failed_configs[device.fqdn] = err
            return
        elif not new_files and not new_json_fragment_files:
            return

        enable_reload = self.args.entire_reload is not cli_args.EntireReloadFlag.no
        force_reload = self.args.entire_reload is cli_args.EntireReloadFlag.force

        upload_files: Dict[str, bytes] = {}
        reload_cmds: Dict[str, bytes] = {}
        generator_types: Dict[str, GeneratorType] = {}
        differ = file_differ_connector.get()
        for generator_type, pc_files in [
            (GeneratorType.ENTIRE, new_files),
            (GeneratorType.JSON_FRAGMENT, new_json_fragment_files),
        ]:
            for file, (file_content_or_json_cfg, cmds) in pc_files.items():
                if generator_type == GeneratorType.ENTIRE:
                    file_content: str = file_content_or_json_cfg
                    diff_content = "\n".join(differ.diff_file(res.device.hw, file, old_files.get(file), file_content))
                else:  # generator_type == GeneratorType.JSON_FRAGMENT
                    old_json_cfg = old_json_fragment_files[file]
                    json_patch = jsontools.make_patch(old_json_cfg, file_content_or_json_cfg)
                    file_content = jsontools.format_json(json_patch)
                    old_text = jsontools.format_json(old_json_cfg)
                    new_text = jsontools.format_json(file_content_or_json_cfg)
                    diff_content = "\n".join(differ.diff_file(res.device.hw, file, old_text, new_text))

                if diff_content or force_reload:
                    self._has_diff |= True

                    upload_files[file] = file_content.encode()
                    generator_types[file] = generator_type
                    self.cmd_lines.append("= %s/%s " % (device.hostname, file))
                    self.cmd_lines.extend([file_content, ""])
                    self.diff_lines.append("= %s/%s " % (device.hostname, file))
                    self.diff_lines.extend([diff_content, ""])

                    if enable_reload:
                        reload_cmds[file] = cmds.encode()
                        self.cmd_lines.append("= Deploy cmds %s/%s " % (device.hostname, file))
                        self.cmd_lines.extend([cmds, ""])

        if self._has_diff:
            self.deploy_cmds[device] = {
                "files": upload_files,
                "cmds": reload_cmds,
                "generator_types": generator_types,
            }
            self.diffs[device] = upload_files
            deployer_driver = annet.deploy.get_deployer()
            before, after = deployer_driver.build_configuration_cmdlist(device.hw)
            for cmd in deployer_driver.build_exit_cmdlist(device.hw):
                after.add_cmd(cmd)
            cmds_pre_files = {}
            rules = rulebook.get_rulebook(device.hw)["deploying"]
            for file, content in self.deploy_cmds[device]["files"].items():
                rule = deploying.match_deploy_rule(rules, [file], content)
                before_more, after_more = annet.deploy.make_apply_commands(
                    rule, res.device.hw, do_commit=True, do_finalize=True, path=file
                )

                cmds_pre_files[file] = "\n".join(map(str, chain(before, before_more))).encode(encoding="utf-8")
                after_cmds = "\n".join(map(str, chain(after, after_more))).encode(encoding="utf-8")
                if after_cmds:
                    self.deploy_cmds[device]["cmds"][file] += b"\n" + after_cmds
            self.deploy_cmds[device]["cmds_pre_files"] = cmds_pre_files


class Deployer:
    def __init__(self, args: cli_args.DeployOptions):
        self.args = args

        self.cmd_lines = []
        self.deploy_cmds = odict()
        self.diffs = {}
        self.failed_configs: Dict[str, Exception] = {}
        self.fqdn_to_device: Dict[str, Device] = {}
        self.empty_diff_hostnames: Set[str] = set()

        self._collapseable_diffs = {}
        self._diff_lines: List[str] = []

    def parse_result(self, job: DeployerJob, result: ann_gen.OldNewResult):
        logger = get_logger(job.device.hostname)

        job.parse_result(result)
        self.failed_configs.update(job.failed_configs)

        if job.has_diff():
            self.cmd_lines.extend(job.cmd_lines)
            self.deploy_cmds.update(job.deploy_cmds)
            self.diffs.update(job.diffs)

            self.fqdn_to_device[result.device.fqdn] = result.device
            self._collapseable_diffs.update(job.collapseable_diffs())
            self._diff_lines.extend(job.diff_lines)
        else:
            logger.info("empty diff")

    def diff_lines(self) -> List[str]:
        diff_lines = []
        diff_lines.extend(self._diff_lines)
        for devices, diff_obj in ann_diff.collapse_diffs(self._collapseable_diffs).items():
            if not diff_obj:
                self.empty_diff_hostnames.update(dev.hostname for dev in devices)
            if not self.args.no_ask_deploy:
                # разобьём список устройств на несколько линий
                dest_name = ""
                try:
                    _, term_columns_str = os.popen("stty size", "r").read().split()
                    term_columns = int(term_columns_str)
                except Exception:
                    term_columns = 2**32
                fqdns = [dev.hostname for dev in devices]
                while fqdns:
                    fqdn = fqdns.pop()
                    if len(dest_name) == 0:
                        dest_name = "= %s" % fqdn
                    elif len(dest_name) + len(fqdn) < term_columns:
                        dest_name = "%s, %s" % (dest_name, fqdn)
                    else:
                        diff_lines.extend([dest_name])
                        dest_name = "= %s" % fqdn
                    if not fqdns:
                        diff_lines.extend([dest_name, ""])
            else:
                dest_name = "= %s" % ", ".join([dev.hostname for dev in devices])
                diff_lines.extend([dest_name, ""])

            for line in registry_connector.get().match(devices[0].hw).make_formatter().diff(diff_obj):
                diff_lines.append(line)
            diff_lines.append("")
        return diff_lines

    def ask_deploy(self) -> str:
        return self._ask(
            "y",
            annet.deploy_ui.AskConfirm(
                text="\n".join(self.diff_lines()),
                alternative_text="\n".join(self.cmd_lines),
            ),
        )

    def ask_rollback(self) -> str:
        return self._ask(
            "n",
            annet.deploy_ui.AskConfirm(
                text="Execute rollback?\n",
                alternative_text="",
            ),
        )

    def _ask(self, default_ans: str, ask: annet.deploy_ui.AskConfirm) -> str:
        # если filter_acl из stdin то с ним уже не получится работать как с терминалом
        ans = default_ans
        if not self.args.no_ask_deploy:
            try:
                if not os.isatty(sys.stdin.fileno()):
                    pts_path = os.ttyname(sys.stdout.fileno())
                    pts = open(pts_path, "r")  # pylint: disable=consider-using-with
                    os.dup2(pts.fileno(), sys.stdin.fileno())
            except OSError:
                pass
            ans = ask.loop()
        return ans

    def check_diff(self, result: annet.deploy.DeployResult, loader: ann_gen.Loader):
        diff_args = self.args.copy_from(
            self.args,
            config="running",
        )
        if not diff_args.query:
            return

        # clear cache
        ann_gen.live_configs = None

        # collect new diffs for devices on which we had successfully uploaded something
        success_device_ids = []
        for host, hres in result.results.items():
            if not isinstance(hres, Exception) and host not in self.empty_diff_hostnames:
                device = self.fqdn_to_device[host]
                success_device_ids.append(device.id)
        diffs, failed = diff(diff_args, loader, success_device_ids)
        for device_id, exc in failed.items():
            self.failed_configs[loader.get_device(device_id).fqdn] = exc

        # "collapse" non-PC diffs
        diffs_by_device_id = ann_diff.collapse_diffs(
            {
                loader.get_device(device_id): diff
                for device_id, diff in diffs.items()
                if diff and not isinstance(diff, PCDiff)
            }
        )
        # add PC diffs as is
        diffs_by_device_id.update(
            {
                (loader.get_device(device_id),): diff
                for device_id, diff in diffs.items()
                if diff and isinstance(diff, PCDiff)
            }
        )
        if diffs_by_device_id:
            print("The diff is still present:")
            for devices, diff_obj in diffs_by_device_id.items():
                for dev in devices:
                    self.failed_configs[dev.fqdn] = Warning("Deploy OK, but diff still exists")
                if isinstance(diff_obj, PCDiff):
                    for diff_file in diff_obj.diff_files:
                        print_err_label(diff_file.label)
                        print("\n".join(format_file_diff(diff_file.diff_lines)))
                else:
                    output_driver = output_driver_connector.get()
                    dest_name = ", ".join([output_driver.cfg_file_names(dev)[0] for dev in devices])
                    print_err_label(dest_name)
                    _print_pre_as_diff(patching.make_pre(diff_obj), diff_args.show_rules, diff_args.indent)


def deploy(
    args: cli_args.DeployOptions,
    loader: ann_gen.Loader,
    deployer: Deployer,
    filterer: Filterer,
    fetcher: Fetcher,
    deploy_driver: DeployDriver,
) -> ExitCode:
    return annet.lib.do_async(adeploy(args, loader, deployer, filterer, fetcher, deploy_driver))


async def adeploy(
    args: cli_args.DeployOptions,
    loader: ann_gen.Loader,
    deployer: Deployer,
    filterer: Filterer,
    fetcher: Fetcher,
    deploy_driver: DeployDriver,
) -> ExitCode:
    """Сгенерировать конфиг для устройств и задеплоить его"""
    ret: ExitCode = 0
    ann_gen.live_configs = await fetcher.fetch(
        devices=loader.devices, processes=args.parallel, max_slots=args.max_slots
    )

    device_ids = [d.id for d in loader.devices]
    for res in ann_gen.old_new(
        args,
        config=args.config,
        loader=loader,
        no_new=args.clear,
        stdin=args.stdin(filter_acl=args.filter_acl, config=args.config),
        do_files_download=True,
        device_ids=device_ids,
        filterer=filterer,
    ):
        # Меняем exit code если хоть один device ловил exception
        if res.err is not None:
            if not args.tolerate_fails:
                raise res.err
            get_logger(res.device.hostname).error("error generating configs", exc_info=res.err)
            ret |= 2**3
        job = DeployerJob.from_device(res.device, args)
        deployer.parse_result(job, res)

    deploy_cmds = deployer.deploy_cmds
    result = annet.deploy.DeployResult(hostnames=[], results={}, durations={}, original_states={})
    if deploy_cmds:
        ans = deployer.ask_deploy()
        if ans != "y":
            return 2**2

        if sys.stdout.isatty() and not args.no_progress:
            progress_bar = annet.deploy_ui.ProgressBars(odict([(device.fqdn, {}) for device in deploy_cmds]))
            progress_bar.init()
            with progress_bar:
                progress_bar.start_terminal_refresher()
                result = await deploy_driver.bulk_deploy(deploy_cmds, args, progress_bar=progress_bar)
                await progress_bar.wait_for_exit()
            progress_bar.screen.clear()
            progress_bar.stop_terminal_refresher()
        else:
            result = await deploy_driver.bulk_deploy(deploy_cmds, args)

    rolled_back = False
    rollback_cmds = {deployer.fqdn_to_device[x]: cc for x, cc in result.original_states.items() if cc}
    if args.rollback and rollback_cmds:
        ans = deployer.ask_rollback()
        if rollback_cmds and ans == "y":
            rolled_back = True
            await deploy_driver.bulk_deploy(rollback_cmds, args)

    if not args.no_check_diff and not rolled_back:
        deployer.check_diff(result, loader)

    if deployer.failed_configs:
        result.add_results(deployer.failed_configs)
        ret |= 2**1

    annet.deploy.show_bulk_report(result.hostnames, result.results, result.durations, log_dir=None)
    for host_result in result.results.values():
        if isinstance(host_result, Exception):
            ret |= 2**0
            break
    return ret


def file_diff(args: cli_args.FileDiffOptions):
    """Создать дифф по рулбуку между файлами или каталогами"""
    old_new = list(_read_old_new_cfgdumps(args))
    pool = Parallel(file_diff_worker, args).tune_args(args)
    return pool.run(old_new, tolerate_fails=True)


def file_diff_worker(
    old_new: Tuple[str, str], args: cli_args.FileDiffOptions
) -> Generator[Tuple[str, str, bool], None, None]:
    old_path, new_path = old_new
    hw = args.hw
    if isinstance(args.hw, str):
        hw = HardwareView(args.hw, "")

    if os.path.isdir(old_path) and os.path.isdir(new_path):
        hostname = os.path.basename(new_path)
        new_files = {
            relative_cfg_path: (cfg_text, "")
            for relative_cfg_path, cfg_text in ann_gen.load_pc_config(new_path).items()
        }
        old_files = ann_gen.load_pc_config(old_path)
        for diff_file in ann_diff.pc_diff(hw, hostname, old_files, new_files):
            diff_text = (
                "\n".join(diff_file.diff_lines) if args.no_color else "\n".join(format_file_diff(diff_file.diff_lines))
            )
            if diff_text:
                yield diff_file.label, diff_text, False
    else:
        old_config, new_config = _read_old_new_configs(old_path, new_path, args.include_missing)
        if old_config == new_config:
            return

        old, new, hw = _read_old_new_hw(old_path, old_config, new_path, new_config, args)
        _, __, pre, ___ = _read_old_new_diff_patch(old, new, hw, add_comments=False)

        if diff_lines := ann_diff.gen_pre_as_diff(pre, args.show_rules, args.indent, args.no_color):
            yield os.path.basename(new_path), "".join(diff_lines), False


@tracing.function
def file_patch(args: cli_args.FilePatchOptions):
    """Создать патч между файлами или каталогами"""
    old_new = list(_read_old_new_cfgdumps(args))
    pool = Parallel(file_patch_worker, args).tune_args(args)
    return pool.run(old_new, tolerate_fails=True)


def file_patch_worker(
    old_new: Tuple[str, str], args: cli_args.FileDiffOptions
) -> Generator[Tuple[str, str, bool], None, None]:
    old_path, new_path = old_new
    if os.path.isdir(old_path) and os.path.isdir(new_path):
        for relative_cfg_path, cfg_text in ann_gen.load_pc_config(new_path).items():
            label = os.path.join(os.path.basename(new_path), relative_cfg_path)
            yield label, cfg_text, False
    else:
        old_config, new_config = _read_old_new_configs(old_path, new_path, args.include_missing)
        if old_config == new_config:
            return

        old, new, hw = _read_old_new_hw(old_path, old_config, new_path, new_config, args)
        _, __, ___, patch_tree = _read_old_new_diff_patch(old, new, hw, args.add_comments)
        patch_text = _format_patch_blocks(patch_tree, hw, args.indent)
        if patch_text:
            yield os.path.basename(new_path), patch_text, False


def guess_hw(config_text: str):
    """Пытаемся угадать вендора и hw на основе
    текста конфига и annet/rulebook/texts/*.rul"""
    scores = []
    hw_provider = hardware_connector.get()
    vendor_registry = registry_connector.get()
    for vendor in vendor_registry:
        hw = hw_provider.vendor_to_hw(vendor)
        rb = rulebook.get_rulebook(hw)
        fmtr = vendor_registry[vendor].make_formatter()

        try:
            config = tabparser.parse_to_tree(config_text, fmtr.split)
        except Exception:
            continue

        pre = patching.make_pre(patching.make_diff({}, config, rb, []))
        metric = _count_pre_score(pre)
        scores.append((hw, metric))

    if not scores:
        raise RuntimeError("No formatter was guessed")

    scores.sort(key=lambda x: (x[1], x[0].vendor), reverse=True)
    return scores[0]


def _count_pre_score(top_pre) -> float:
    """Обходим вширь pre-конфиг
    и подсчитываем количество заматчившихся
    правил на каждом из уровней.

    Чем больше результирующий приоритет
    тем больше рулбук соответсвует конфигу.
    """
    score = 0
    scores = []
    cur, child = [top_pre], []
    while cur:
        for pre in cur.pop().values():
            score += 1
            for item in pre["items"].values():
                for op in [Op.ADDED, Op.AFFECTED, Op.REMOVED]:
                    child += [x["children"] for x in item[op]]
        if not cur:
            scores.append(score)
            score = 0
            cur, child = child, []
    result = 0
    for i in reversed(scores):
        result <<= i.bit_length()
        result += i
    if result > 0:
        result = 1 - (1 / result)
    return float(result)
