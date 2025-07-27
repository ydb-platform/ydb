#! /usr/bin/python3

from __future__ import annotations
import json
import logging
import sys
import os
from enum import StrEnum


class Resolution(StrEnum):
    NO = ''
    OK = '#aaffaa'
    INFO = '#aaffaa'
    WARNING = '#ffffaa'
    ERROR = '#ffaaaa'


class FieldInfo:
    def __init__(self, field: dict):
        self.id = field.get('id')
        self.value = field.get('default-value')


class Differ:

    def __init__(self):
        self.fields: list[tuple[str, list[FieldInfo]]] = []
        self.resolutions: list[tuple[str, list[tuple[Resolution, str]]]] = []
        self.names: list[str] = []

    def load_files(self, names: list[str]):
        configs = []
        for name in names:
            with open(name) as f:
                configs.append(json.load(f).get('proto'))
            self.names.append(os.path.basename(name))
        self._add_fields_dict(configs, [])

    def _add_fields_dict(self, fields: list[dict], path: list[str]):
        keys = set()
        for f in fields:
            if isinstance(f, dict):
                for key in f.keys():
                    keys.add(key)

        for k in sorted(keys):
            key_fields = [f.get(k) if isinstance(f, dict) else None for f in fields]
            self._add_fields(key_fields, path + [k])

    def _add_fields(self, fields: list[dict], path: list[str]):
        maxlist = 0
        dicts = []
        infos = []
        for f in fields:
            if f is None:
                infos.append(None)
                dicts.append(None)
                continue
            info = FieldInfo(f)
            infos.append(info)
            dicts.append(info.value if isinstance(info.value, dict) else None)
            if isinstance(info.value, list):
                maxlist = max(maxlist, len(info.value))
        self.fields.append(('.'.join(path), infos))
        self._add_fields_dict(dicts, path)
        for i in range(maxlist):
            index_fields = [info.value[i] if info and isinstance(info.value, list) and len(info.value) > i else None for info in infos]
            self._add_fields(index_fields, path + [str(i)])

    def compare_two_fields(self, old: FieldInfo, new: FieldInfo, path: str) -> tuple[Resolution, str]:
        if old is None and new is None:
            return Resolution.NO, ''
        if old is None:
            if isinstance(new.value, dict):
                value = '{}'
            elif isinstance(new.value, list):
                value = '[]'
            else:
                value = new.value
            return Resolution.INFO, f'added <b>{value}</b>'
        if new is None:
            return Resolution.ERROR, 'deleted'
        if old.id != new.id:
            return Resolution.ERROR, f'id changed<br/>{old.id} -> {new.id}'
        if type(old.value) is not type(new.value):
            return Resolution.ERROR, 'type changed'
        if isinstance(old.value, dict) or isinstance(old.value, list):
            return Resolution.OK, ''
        if old.value != new.value:
            if path.startswith('FeatureFlags.') and isinstance(old.value, bool):
                if not old.value:
                    return Resolution.INFO, 'FF switched on'
                else:
                    return Resolution.ERROR, 'FF switched off'
            return Resolution.WARNING, f'value changed<br/>{old.value} -> {new.value}'
        return Resolution.OK, ''

    def compare(self):
        for name, values in self.fields:
            if len(values) == 0:
                continue
            result = [(Resolution.NO if values[0] is None else Resolution.OK, '')]
            intresting = False
            for i in range(1, len(values)):
                result.append(self.compare_two_fields(values[i-1], values[i], name))
                intresting = intresting or result[-1][0] not in {Resolution.OK, Resolution.NO, Resolution.INFO}
            if intresting:
                self.resolutions.append((name, result))

    def print_result(self) -> None:
        print('<html><body><table border=1 valign="center">')
        print('<thead style="position: sticky; top: 0; background: white; align: center"><tr><th style="padding-left: 10; padding-right: 10">field</th>')
        for name in self.names:
            print(f'<th style="padding-left: 10; padding-right: 10">{name}</th>')
        print('</tr></thead>')
        print('<tbody>')
        for field, result in self.resolutions:
            print(f'<tr><td style="padding-left: 10; padding-right: 10">{field}</td>')
            for color, msg in result:
                print(f'<td align="center" bgcolor="{color}" style="padding-left: 10; padding-right: 10">{msg}</td>')
            print('</tr>')
        print('</tbody>')
        print('</table></body></html>')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    differ = Differ()
    differ.load_files(sys.argv[1:])
    differ.compare()
    differ.print_result()
