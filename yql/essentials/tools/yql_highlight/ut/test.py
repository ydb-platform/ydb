#!/usr/bin/env python3

import json
import re

def read(path):
    with open(path, 'r') as f:
        content = f.read()
        if path.startswith('query'):
            content = content.replace('\\', '\\\\')
            content = content.replace('`', '\\`')
        return content


content = read('test.template.html')
for file in (
    'YQL.monarch.json',
    'YQLs.monarch.json',
    'YQL.tmLanguage.json',
    'YQLs.tmLanguage.json',
    'YQL.highlightjs.json',
    'YQLs.highlightjs.json',
    'query.yql',
    'query.yqls',
):
    content = content.replace('/* {{%s}} */' % file, read(file))

with open('test.patched.html', 'w') as f:
    f.write(content)
