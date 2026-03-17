import abc
import difflib
import os
import re
from itertools import groupby
from pathlib import Path
from typing import Any, Dict, Generator, List, Mapping, Optional, Protocol, Tuple, Union

from annet import cli_args, filtering, patching, rulebook
from annet.annlib import jsontools
from annet.annlib.diff import (  # pylint: disable=unused-import
    colorize_line,
    diff_cmp,
    diff_ops,
    gen_pre_as_diff,
    resort_diff,
)
from annet.annlib.netdev.views.hardware import HardwareView
from annet.annlib.output import LABEL_NEW_PREFIX, format_file_diff
from annet.cli_args import ShowDiffOptions
from annet.connectors import CachedConnector
from annet.output import output_driver_connector
from annet.storage import Device
from annet.types import Diff, PCDiff, PCDiffFile
from annet.vendors import registry_connector, tabparser

from .gen import Loader, old_new


def _diff_files(hw, old_files, new_files):
    ret = {}
    differ = file_differ_connector.get()
    for path, (new_text, reload_data) in new_files.items():
        old_text = old_files.get(path)
        is_new = old_text is None
        diff_lines = differ.diff_file(hw, path, old_text, new_text)
        ret[path] = (diff_lines, reload_data, is_new)
    return ret


def pc_diff(
    hw, hostname: str, old_files: Dict[str, str], new_files: Dict[str, str]
) -> Generator[PCDiffFile, None, None]:
    sorted_lines = sorted(_diff_files(hw, old_files, new_files).items())
    for path, (diff_lines, _reload_data, is_new) in sorted_lines:
        if not diff_lines:
            continue
        label = hostname + os.sep + path
        if is_new:
            label = LABEL_NEW_PREFIX + label
        yield PCDiffFile(label=label, diff_lines=diff_lines)


def json_fragment_diff(
    hw,
    hostname: str,
    old_files: Dict[str, Any],
    new_files: Dict[str, Tuple[Any, Optional[str]]],
) -> Generator[PCDiffFile, None, None]:
    def jsonify_multi(files):
        return {path: jsontools.format_json(cfg) for path, cfg in files.items()}

    def jsonify_multi_with_cmd(files):
        ret = {}
        for path, cfg_reload_cmd in files.items():
            cfg, reload_cmd = cfg_reload_cmd
            ret[path] = (jsontools.format_json(cfg), reload_cmd)
        return ret

    jold, jnew = jsonify_multi(old_files), jsonify_multi_with_cmd(new_files)
    return pc_diff(hw, hostname, jold, jnew)


def worker(
    device_id,
    args: cli_args.DiffOptions,
    stdin,
    loader: Loader,
    filterer: filtering.Filterer,
) -> Union[Diff, PCDiff, None]:
    for res in old_new(
        args,
        config=args.config,
        loader=loader,
        no_new=args.clear,
        do_files_download=True,
        device_ids=[device_id],
        filterer=filterer,
        stdin=stdin,
    ):
        old = res.get_old(args.acl_safe)
        new = res.get_new(args.acl_safe)
        device = res.device
        acl_rules = res.get_acl_rules(args.acl_safe)
        new_files = res.get_new_files(args.acl_safe)
        new_json_fragment_files = res.get_new_file_fragments(args.acl_safe)

        pc_diff_files = []
        if res.old_files or new_files:
            pc_diff_files.extend(pc_diff(res.device.hw, device.hostname, res.old_files, new_files))
        if res.old_json_fragment_files or new_json_fragment_files:
            pc_diff_files.extend(
                json_fragment_diff(res.device.hw, device.hostname, res.old_json_fragment_files, new_json_fragment_files)
            )

        if pc_diff_files:
            pc_diff_files.sort(key=lambda f: f.label)
            return PCDiff(hostname=device.hostname, diff_files=pc_diff_files)
        elif old is not None:
            orderer = patching.Orderer.from_hw(device.hw)
            rb = rulebook.get_rulebook(device.hw)
            diff_tree = patching.make_diff(
                old,
                orderer.order_config(new),
                rb,
                [acl_rules, res.filter_acl_rules],
            )
            diff_tree = patching.strip_unchanged(diff_tree)
            return diff_tree


def gen_sort_diff(
    diffs: Mapping[Device, Union[Diff, PCDiff]], args: ShowDiffOptions
) -> Generator[Tuple[str, Generator[str, None, None] | str, bool], None, None]:
    """
    Возвращает осортированный дифф, совместимый с write_output
    :param diffs: Маппинг устройства в дифф
    :param args: Параметры коммандной строки
    """
    if args.no_collapse:
        devices_to_diff = {(dev,): diff for dev, diff in diffs.items()}
    else:
        non_pc_diffs = {dev: diff for dev, diff in diffs.items() if not isinstance(diff, PCDiff)}
        devices_to_diff = collapse_diffs(non_pc_diffs)
        devices_to_diff.update({(dev,): diff for dev, diff in diffs.items() if isinstance(diff, PCDiff)})
    for devices, diff_obj in devices_to_diff.items():
        if not diff_obj:
            continue
        if isinstance(diff_obj, PCDiff):
            for diff_file in diff_obj.diff_files:
                diff_text = (
                    "\n".join(diff_file.diff_lines)
                    if args.no_color
                    else "\n".join(format_file_diff(diff_file.diff_lines))
                )
                yield diff_file.label, diff_text, False
        else:
            output_driver = output_driver_connector.get()
            dest_name = ", ".join([output_driver.cfg_file_names(dev)[0] for dev in devices])
            pd = patching.make_pre(resort_diff(diff_obj))
            yield dest_name, gen_pre_as_diff(pd, args.show_rules, args.indent, args.no_color), False


def _transform_text_diff_for_collapsing(text_diff) -> List[str]:
    for line_no, line in enumerate(text_diff):
        text_diff[line_no] = re.sub(r"(snmp-agent .+) cipher \S+ (.+)", r"\1 cipher ENCRYPTED \2", line)
    return text_diff


def _make_text_diff(device: Device, diff: Diff) -> List[str]:
    formatter = registry_connector.get().match(device.hw).make_formatter()
    res = formatter.diff(diff)
    return res


def collapse_diffs(diffs: Mapping[Device, Diff]) -> Dict[Tuple[Device, ...], Diff]:
    """
    Группировка диффов.
    :param diffs:
    :return: дикт аналогичный типу Diff, но с несколькими dev в ключе.
        Нужно учесть что дифы сверяются в отформатированном виде
    """
    diffs_with_test = {
        dev: [diff, _transform_text_diff_for_collapsing(_make_text_diff(dev, diff))] for dev, diff in diffs.items()
    }
    res = {}
    for _, collapsed_diff_iter in groupby(
        sorted(diffs_with_test.items(), key=lambda x: (x[0].hw.vendor, x[1][1])), key=lambda x: x[1][1]
    ):
        collapsed_diff = list(collapsed_diff_iter)
        res[tuple(x[0] for x in collapsed_diff)] = collapsed_diff[0][1][0]

    return res


class FileDiffer(Protocol):
    @abc.abstractmethod
    def diff_file(self, hw: HardwareView, path: str | Path, old: str, new: str) -> list[str]:
        raise NotImplementedError


class UnifiedFileDiffer(FileDiffer):
    def __init__(self):
        self.context: int = 3

    def diff_file(self, hw: HardwareView, path: str | Path, old: str, new: str) -> list[str]:
        """Calculate the differences for config files.

        Args:
            hw: device hardware info
            path: path to file on a device
            old (Optional[str]): The old file content.
            new (Optional[str]): The new file content.

        Returns:
            List[str]: List of difference lines.
        """
        return self._diff_text_file(old, new)

    def _diff_text_file(self, old, new):
        """Calculate the differences for plaintext files."""
        context = self.context
        old_lines = old.splitlines() if old else []
        new_lines = new.splitlines() if new else []
        context = max(len(old_lines), len(new_lines)) if context is None else context
        return list(difflib.unified_diff(old_lines, new_lines, n=context, lineterm=""))


class FrrFileDiffer(UnifiedFileDiffer):
    def diff_file(self, hw: HardwareView, path: str | Path, old: str, new: str) -> list[str]:
        if (hw.PC.Mellanox or hw.PC.NVIDIA) and (path == "/etc/frr/frr.conf"):
            return self._diff_frr_conf(hw, old, new)
        return super().diff_file(hw, path, old, new)

    def _diff_frr_conf(self, hw: HardwareView, old_text: str | None, new_text: str | None) -> list[str]:
        """Calculate the differences for frr.conf files."""
        indent = "  "
        rb = rulebook.rulebook_provider_connector.get()
        rulebook_data = rb.get_rulebook(hw)
        formatter = registry_connector.get().match(hw).make_formatter(indent=indent)

        old_tree = tabparser.parse_to_tree(old_text or "", splitter=formatter.split)
        new_tree = tabparser.parse_to_tree(new_text or "", splitter=formatter.split)

        diff_tree = patching.make_diff(old_tree, new_tree, rulebook_data, [])
        pre_diff = patching.make_pre(diff_tree)
        diff_iterator = gen_pre_as_diff(pre_diff, show_rules=False, indent=indent, no_color=True)

        return [line.rstrip() for line in diff_iterator if "frr version" not in line]


class _FileDifferConnector(CachedConnector[FileDiffer]):
    name = "Device file diff processor"
    ep_name = "file_differ"


file_differ_connector = _FileDifferConnector()
