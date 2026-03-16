import abc
import os
import posixpath
import sys
from typing import Dict, List, Optional, Tuple, Type
from urllib.parse import urlparse

import colorama
from contextlog import get_logger

from annet.annlib.output import (  # pylint: disable=unused-import
    LABEL_NEW_PREFIX,
    OutputWriter,
    TextArgs,
    capture_output,
    dir_or_file_output,
    format_file_diff,
    print_as_json,
    print_as_yaml,
    print_err_label,
    print_label,
)
from annet.cli_args import FileOutOptions, QueryOptions
from annet.connectors import Connector
from annet.storage import Device, storage_connector


BLACKBOX_FILENAME = "config.cfg"


class _DriverConnector(Connector["OutputDriver"]):
    name = "OutputDriver"
    ep_name = "output"
    ep_by_group_only = "annet.connectors.output"

    def _get_default(self) -> Type["OutputDriver"]:
        return OutputDriverBasic


output_driver_connector = _DriverConnector()


class OutputDriver(abc.ABC):
    @abc.abstractmethod
    def write_output(self, arg_out: FileOutOptions, items, query_result_count=1) -> None:
        pass

    @abc.abstractmethod
    def format_fails(self, fail, fqdns: Optional[Dict[int, str]] = None) -> Tuple[str, str]:
        pass

    @abc.abstractmethod
    def cfg_file_names(self, device: Device) -> List[str]:
        pass

    @abc.abstractmethod
    def entire_config_dest_path(self, device: Device, config_path: str) -> str:
        pass


class OutputDriverBasic(OutputDriver):
    def write_output(self, arg_out: FileOutOptions, items, query_result_count=1):
        """
        пишет результаты генерации в файл или директорию :dest
        :dest - это директория в случаях:
        - заканчивается на "/"
        - существует директория с таким именем
        - более одного устройства в результате запроса
        - есть entire-генераторы (определяется по типу первого результата, если устройство одно)
        """
        logger = get_logger()

        items_iter = iter(items)
        try:
            first_result = next(items_iter)
        except StopIteration:
            # нет результатов, ничего не пишем и не создаём
            return

        def _reassemble_items():
            yield first_result
            yield from items_iter

        dest = arg_out.dest
        suggest_dir = arg_out.dest_force_create_dir or os.sep in first_result[0]
        dir_mode = dir_or_file_output(dest, query_result_count, suggest_dir=suggest_dir)
        _reassembled_items = list(_reassemble_items())

        for output_no, (label, output, is_fail) in enumerate(_reassembled_items):
            writer = output if isinstance(output, OutputWriter) else OutputWriter(output)
            label = os.path.normpath(label)
            label_color = colorama.Back.RED if is_fail else colorama.Back.GREEN
            if dest is None:
                if hasattr(arg_out, "format") and arg_out.format == "json":
                    if output_no > 0:
                        sys.stdout.write(",")
                    elif output_no == 0:
                        sys.stdout.write("{")
                    sys.stdout.write('"%s": ' % label)
                    writer.write(sys.stdout)
                    if output_no == len(_reassembled_items) - 1:
                        sys.stdout.write("}")
                else:
                    if not arg_out.no_label:
                        print_label(label, back_color=label_color)
                    writer.write(sys.stdout)
            elif dir_mode:
                if arg_out.dest_force_create_dir and os.sep not in label:
                    label = os.path.join(label, BLACKBOX_FILENAME)

                if label.startswith(LABEL_NEW_PREFIX):
                    label = label[len(LABEL_NEW_PREFIX) :]
                if label.startswith(os.sep):
                    # just in case.
                    label = label.lstrip(os.sep)

                if os.sep not in label:
                    # vendor config
                    label = os.path.basename(label)

                else:
                    # entire generated file
                    parts = label.split(os.sep)
                    hostname = parts[0]
                    label = os.sep.join(parts[1:])
                    if not arg_out.expand_path:
                        label = os.path.basename(label)
                    if query_result_count > 1 or arg_out.dest_force_create_dir:
                        label = os.path.join(hostname, label)
                file_dest = os.path.join(dest, "errors") if is_fail else dest

                out_file = os.path.normpath(os.path.join(file_dest, label))
                logger.info("writing '%s'", out_file)
                dirname = os.path.dirname(out_file)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                with open(out_file, "w") as file:
                    writer.write(file)
            else:
                logger.info("writing '%s'", dest)
                with open(dest, "w") as file:
                    writer.write(file)

    def format_fails(self, fail, fqdns: Optional[Dict[int, str]] = None):
        ret = []
        fqdns = fqdns or {}
        for assignment, exc in fail.items():
            label = assignment
            if assignment in fqdns:
                label = fqdns[assignment]
            elif isinstance(assignment, tuple):
                label = assignment[0]
            else:
                ValueError("Failed to parse failed assignment %r" % assignment)
            ret.append((label, getattr(exc, "formatted_output", f"{repr(exc)} (formatted_output is absent)"), True))
        return ret

    def cfg_file_names(self, device: Device) -> list[str]:
        res = []
        if device.hostname:
            res.append(f"{device.hostname}.cfg")
        if device.id is not None and device.id != "":
            res.append(f"_id_{device.id}.cfg")
        if not res:
            raise RuntimeError("Neither hostname nor id is known for device")
        return res

    def entire_config_dest_path(self, device, config_path: str) -> str:
        """Формирует путь к конфигу в директории destname.

        Например, для устройства с hostname `my-device`:
        ```
        >>> device.entire_config_dest_path("/etc/frr/frr.conf")
        'my-device.cfg/etc/frr/frr.conf'
        >>>
        ```
        """

        # NOTE: с полученным `config_path` работаем через `posixpath`, а не через `os.path`, потому что
        #       entire-путь POSIX-специфичный; но в конце формируем путь через `os.path` для текущей платформы
        if not posixpath.abspath(config_path):
            raise RuntimeError(f"Want absolute config path, but relative received: {config_path}")

        cfg_files = self.cfg_file_names(device)

        parsed_config_path = urlparse(config_path)
        scheme, config_path = parsed_config_path.scheme or "file", parsed_config_path.path

        relative_config_path = posixpath.relpath(config_path, "/")
        if scheme != "file":
            host = parsed_config_path.hostname + f":{parsed_config_path.port}" if parsed_config_path.port else ""
            relative_config_path = os.path.join(host, relative_config_path)

        dest_config_path_parts = [cfg_files[0]] + relative_config_path.split(posixpath.sep)

        return os.path.join(*dest_config_path_parts)
