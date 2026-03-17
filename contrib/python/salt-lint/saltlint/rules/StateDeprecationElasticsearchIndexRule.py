# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

from saltlint.linter.rule import DeprecationRule


class StateDeprecationElasticsearchIndexRule(DeprecationRule):
    id = '902'
    state = 'elasticsearch_index.absent'
    deprecated_since = '2017.7.0'
    version_added = 'v0.5.0'
