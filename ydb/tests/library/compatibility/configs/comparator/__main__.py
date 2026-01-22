#! /usr/bin/python3

from __future__ import annotations
import json
import logging
import sys
import os
from enum import Enum


class Resolution(Enum):
    NO = 0
    OK = 1
    INFO = 2
    WARNING = 3
    ERROR = 4


class FieldInfo:
    def __init__(self, field: dict):
        self.id = field.get('id')
        self.value = field.get('default-value')


class FieldIdent:
    def __init__(self, name: str, file: str):
        self.name: str = name
        self.file: str = file


def _get_value_string(value: FieldInfo) -> str:
    if value is None:
        return ''
    if isinstance(value.value, dict):
        return '{}'
    if isinstance(value.value, list):
        return '[]'
    return str(value.value)


class FieldResolution:
    def __init__(self, ident: FieldIdent, first_value: FieldInfo):
        self.ident = ident
        self.result_class: str = ''
        self.branch_resolutions: list[tuple[Resolution, str]] = [(Resolution.NO if first_value is None else Resolution.OK, _get_value_string(first_value))]


class Differ:

    def __init__(self):
        self.fields: list[tuple[FieldIdent, list[FieldInfo]]] = []
        self.resolutions: list[FieldResolution] = []
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
        file = ''
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
            file = f.get('file', file)
        self.fields.append((FieldIdent('.'.join(path), file), infos))
        self._add_fields_dict(dicts, path)
        for i in range(maxlist):
            index_fields = [info.value[i] if info and isinstance(info.value, list) and len(info.value) > i else None for info in infos]
            self._add_fields(index_fields, path + [str(i)])

    def compare_two_fields(self, old: FieldInfo, new: FieldInfo, path: str) -> tuple[Resolution, str]:
        if old is None and new is None:
            return Resolution.NO, ''
        if new is None:
            return Resolution.ERROR, 'deleted'
        value_str = _get_value_string(new)
        if old is None:
            return Resolution.INFO, f'added <b>{value_str}</b>'
        if old.id != new.id:
            return Resolution.ERROR, f'id changed {old.id} -> {new.id}'
        if type(old.value) is not type(new.value):
            return Resolution.ERROR, 'type changed'
        if isinstance(old.value, dict) or isinstance(old.value, list):
            return Resolution.OK, value_str
        if old.value != new.value:
            if path.startswith('FeatureFlags.') and isinstance(old.value, bool):
                if not old.value:
                    return Resolution.INFO, 'FF switched on'
                else:
                    return Resolution.ERROR, 'FF switched off'
            return Resolution.WARNING, f'value changed {old.value} -> {new.value}'
        return Resolution.OK, value_str

    def compare(self):
        for ident, values in self.fields:
            if len(values) == 0:
                continue
            res = FieldResolution(ident, values[0])
            max_resolution = res.branch_resolutions[0][0].value
            for i in range(1, len(values)):
                res.branch_resolutions.append(self.compare_two_fields(values[i-1], values[i], ident.name))
                max_resolution = max(max_resolution, res.branch_resolutions[-1][0].value)
            res.result_class = f'resolution_{max_resolution}'
            self.resolutions.append(res)

    def print_result(self) -> None:
        print('''<style>
.tab {
    overflow: hidden;
    background-color: #AAFFAA;
}

.tab button {
    border: 1px solid #4CAF50;
    float: left;
    cursor: pointer;
    padding: 7px 16px;
    transition: 0.3s;
}

.tab button.active {
    background-color: #888888;
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
        if (tabcontent[i].style.display != "") {
            activeid = tabcontent[i].id
        }
        tabcontent[i].style.display = "";
    }
    tablinks = document.getElementsByClassName("tablinks active");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = "tablinks";
    }
    if (lang == activeid) {
        return;
    }
    document.getElementById(lang).style.display = "block";
    evt.currentTarget.className = "tablinks active";
}

function showSwitchChanged() {
    switches = document.getElementsByClassName("showSwitch");
    res = 0;
    for (i = 0; i < switches.length; i++) {
        if (switches[i].checked) {
            res = Number(switches[i].value);
        }
    }
    for (resolution = 0; resolution < 5; ++resolution) {
        lines = document.getElementsByClassName("resolution_" + resolution);
        for (i = 0; i < lines.length; i++) {
            lines[i].style.display = resolution >= res ? "table-row" : "none"
        }
    }
}

function showBranches(event) {
    if (event.currentTarget.className == "branchbutton active") {
        event.currentTarget.className = "branchbutton"
    } else {
        event.currentTarget.className = "branchbutton active"
    }
    buttons = document.getElementsByClassName("branchbutton active")
    activecols = new Set([0]);
    for (i = 0; i < buttons.length; ++i) {
        activecols.add(Number(buttons[i].value));
    }
    table = document.getElementById("maintable");
    for (r = 0; r < table.rows.length; r++) {
        row = table.rows[r];
        for (c = 0; c < row.children.length; c++) {
            row.children[c].style.display = activecols.has(c) ? "" : "none";
        }
    }
}

</script>''')
        print(f'''
<html>
<head>
<meta charset="utf-8">
<title>Сравнение дефолтных конфигураций ydbd</title>
</head>
<body onLoad="showSwitchChanged()">
<div class="tab">
  <table valign=middle border=0><tr>
    <td><button class="tablinks" onclick="openDescr(event, 'russian')">Описание</button></td>
    <td><button class="tablinks" onclick="openDescr(event, 'english')">Description</button></td>
    <td>Show lines:
      <input id="showAll" class="showSwitch" type="radio" name="show" value={Resolution.OK.value} onchange="showSwitchChanged()"><label for="showAll">All</label>
      <input id="showChanged" class="showSwitch" type="radio" name="show" value={Resolution.INFO.value} onchange="showSwitchChanged()"><label for="showChanged">Changed</label>
      <input id="showSuspicious" class="showSwitch" type="radio" name="show" value={Resolution.WARNING.value} onchange="showSwitchChanged()" checked><label for="showSuspicious">Suspicious</label>
    </td>
    {'\n'.join([f'<td><button class="branchbutton active" onclick="showBranches(event)" value={b + 1}>{self.branches[b]["branch_name"]}</button></td>' for b in range(len(self.branches))])}
  </tr></table>
</div>
<div id=russian class="tabcontent">
<h1>Сравнение дефолтных конфигураций ydbd разных версий</h1>
В таблице ниже представлены изменения дефолтных конфигураций для разных версий ydbd.
Сделано на основе описания конфигураций в <a href="https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/config.proto">протобуфе</a>.
<p>
Цвет означает критичность изменений в соседних ветках:
<ul>
<li><span style="font-weight:bold">Белый</span> означает, что поле полностью отсутствует в данной версии конфигурации.</li>
<li><span style="background-color: #dddddd;font-weight:bold">Серый</span> - значение задано и не изменялось, в том числе пустое.</li>
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
<li><span style="background-color: #dddddd;font-weight:bold">Grey</span> - the value is set and has not changed, including the empty one.</li>
<li><span style="background-color: #aaffaa;font-weight:bold">Green</span> - a safe change, such as adding a new field or enabling the Feature flag.</li>
<li><span style="background-color: #ffffaa;font-weight:bold">Yellow</span> - a change that may cause a change in behavior, but should not be critical, such as changing the default value.</li>
<li><span style="background-color: #ffaaaa;font-weight:bold">Red</span> - A dangerous change that can break everything:
removing fields, changing their type or id (in protobuf), disabling the Feature flag etc.</li>
</ul>
</p>
</div>
<table id="maintable" border=1 valign="center" width="100%">''')
        print('<thead style="position: sticky; top: 0; background: white; align: center">')
        max_width = int(50 / len(self.branches))
        print('<tr><th style="padding-left: 10; padding-right: 10; word-break: break-all; word-wrap: break-word; width: 50%;">config field \\ branch, commit</th>')
        for b in self.branches:
            br_text = b['branch_name']
            c = b.get('commit')
            if c:
                br_text += f'<br/><a href="https://github.com/ydb-platform/ydb/commit/{c}">{c[0:7]}</a>'
            print(f'<th style="padding-left: 10; padding-right: 10; word-break: break-all; word-wrap: break-word; width: {max_width}%;">{br_text}</th>')
        print('</tr></thead>')
        print('<tbody>')
        for res in self.resolutions:
            if res.ident.file:
                field = f'<a href="https://github.com/ydb-platform/ydb/blame/main/{res.ident.file}">{res.ident.name}</a>'
            else:
                field = res.ident.name
            print(f'<tr class="{res.result_class}"><td style="padding-left: 10; padding-right: 10; word-break: break-all; word-wrap: break-word; width: 50%;">{field}</td>')
            for resolution, msg in res.branch_resolutions:
                color = {
                    Resolution.NO: '',
                    Resolution.OK: '#dddddd',
                    Resolution.INFO: '#aaffaa',
                    Resolution.WARNING: '#ffffaa',
                    Resolution.ERROR: '#ffaaaa'
                }[resolution]
                print(f'<td align="center" bgcolor="{color}" style="padding-left: 10; padding-right: 10; word-break: break-all; word-wrap: break-word; width: {max_width}%;">{msg}</td>')
            print('</tr>')
        print('</tbody>')
        print('</table></body></html>')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    differ = Differ()
    differ.load_files(sys.argv[1:])
    differ.compare()
    differ.print_result()
