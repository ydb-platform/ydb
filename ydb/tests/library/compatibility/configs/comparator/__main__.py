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
        self.branches: list[dict[str, str]] = []

    def load_files(self, names: list[str]):
        configs = []
        for name in names:
            with open(name) as f:
                content = json.load(f)
                configs.append(content.get('proto'))
                branch_name = content.get('branch')
                if not branch_name:
                    branch_name = os.path.basename(name)
                self.branches.append({
                    'branch_name' : branch_name,
                    'commit': content.get('commit')
                })
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
        print('''<style>
.tab {
    overflow: hidden;
    border: 1px solid #4CAF50;
    background-color: #AAFFAA;
}


.tab button {
    background-color: inherit;
    float: left;
    border: none;
    outline: none;
    cursor: pointer;
    padding: 14px 16px;
    transition: 0.3s;
}


.tab button:hover {
    background-color: #FFEB3B;
}


.tab button.active {
    background-color: #4CAF50;
  color: #fff;
}


.tabcontent {
    display: none;
    padding: 6px 12px;
    border: 1px solid #4CAF50;
    border-top: none;
}
</style>
<script>
function openDescr(evt, lang) {
    var i, tabcontent, tablinks;

    tabcontent = document.getElementsByClassName("tabcontent");
    activeid = ""
    for (i = 0; i < tabcontent.length; i++) {
        if (tabcontent[i].style.display != "none") {
            activeid = tabcontent[i].id
        }
        tabcontent[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablinks");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = tablinks[i].className.replace(" active", "");
    }
    if (lang == activeid) {
        return;
    }
    document.getElementById(lang).style.display = "block";
    evt.currentTarget.className += " active";
}
</script>

<html>
<head>
<meta charset="utf-8">
<title>Сравнение дефолтных конфигураций ydbd</title>
</head>
<body>
<div class="tab">
  <button class="tablinks" onclick="openDescr(event, 'russian')">Описание</button>
  <button class="tablinks" onclick="openDescr(event, 'english')">Description</button>
</div>
<div id=russian class="tabcontent">
<h1>Сравнение дефолтных конфигураций ydbd разных версий</h1>
В таблице ниже представлены изменения дефолтных конфигураций для разных версий ydbd.
Сделано на основе описания конфигураций в <a href="https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/config.proto">протобуфе</a>.
<p>
Цвет означает критичность изменений в соседних ветках:
<ul>
<li><span style="font-weight:bold">Белый</span> означает, что поле полностью отсутствует в данной версии конфигурации.</li>
<li><span style="background-color: #aaffaa;font-weight:bold">Зеленый</span> - безопасное изменение, такое как добавление нового поля или включение Feature flag.</li>
<li><span style="background-color: #ffffaa;font-weight:bold">Желтый</span> - изменение, которое может вызвать изменение поведения,
но не должно быть критичным, например изменение значения по умолчанию.</li>
<li><span style="background-color: #ffaaaa;font-weight:bold">Красный</span> - опасное изменение,
которое может все сломать: удаление полей, изменение их типа или id (в протобуфе), выключение Feature flag итд.</li>
</ul>
</p>
</div>
<div id=english class="tabcontent">
<h1>Comparison of ydbd default configurations of different versions</h1>
The table above shows the changes in default configurations for different versions of ydbd.
Based on the description of configurations in the <a href="https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/config.proto">protobuf</a>.
<p>
The color indicates the criticality of the changes in the neighboring branches:
<ul>
<li><span style="font-weight:bold">White</span> means that the field is completely absent in this version of the configuration.</li>
<li><span style="background-color: #aaffaa;font-weight:bold">Green</span> - a safe change, such as adding a new field or enabling the Feature flag.</li>
<li><span style="background-color: #ffffaa;font-weight:bold">Yellow</span> - a change that may cause a change in behavior, but should not be critical, such as changing the default value.</li>
<li><span style="background-color: #ffaaaa;font-weight:bold">Red</span> - A danger change that can break everything:
removing fields, changing their type or id (in protobuf), disabling the Feature flag etc.</li>
</ul>
</p>
</div>
<table border=1 valign="center" width="100%">''')
        print('<thead style="position: sticky; top: 0; background: white; align: center">')
        print('<tr><th style="padding-left: 10; padding-right: 10">config field \\ branch, commit</th>')
        for b in self.branches:
            br_text = b['branch_name']
            c = b.get('commit')
            if c:
                br_text += f'<br/><a href="https://github.com/ydb-platform/ydb/commit/{c}">{c[0:7]}</a>'
            print(f'<th style="padding-left: 10; padding-right: 10">{br_text}</th>')
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
