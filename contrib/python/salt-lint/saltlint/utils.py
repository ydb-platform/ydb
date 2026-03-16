# -*- coding: utf-8 -*-
# Copyright (c) 2013-2014 Will Thames <will@thames.id.au>
# Modified work Copyright (c) 2020-2021 Warpnet B.V.

import glob
import importlib.util
import os


LANGUAGE_SLS = "sls"
LANGUAGE_JINJA = "jinja"


def load_plugins(directory, config):
    result = []
    fh = None

    for pluginfile in glob.glob(os.path.join(directory, '[A-Za-z]*.py')):

        pluginname = os.path.basename(pluginfile.replace('.py', ''))
        try:
            spec = importlib.util.spec_from_file_location(pluginname, pluginfile)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            obj = getattr(module, pluginname)(config)
            result.append(obj)
        finally:
            if fh:
                fh.close()
    return result


def get_rule_skips_from_line(line):
    rule_id_list = []
    if '# noqa' in line:
        noqa_text = line.split('# noqa')[1]
        rule_id_list = noqa_text.split()
    return rule_id_list


def get_rule_skips_from_text(text):
    rule_id_list = []
    for line in text.splitlines():
        rule_id_list.extend(get_rule_skips_from_line(line))

    # Return a list of unique ids
    return list(set(rule_id_list))


def get_file_type(file_name):
    extension = os.path.splitext(file_name)[1].lower()

    if extension == ".sls":
        return LANGUAGE_SLS
    if extension in [".jinja", ".jinja2", ".j2"]:
        return LANGUAGE_JINJA
    return None
