# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

from saltlint.linter.rule import DeprecationRule


class StateDeprecationDockerModWatch(DeprecationRule):
    id = '909'
    state = 'docker.mod_watch'
    deprecated_since = '2017.7.0'
    version_added = 'v0.5.0'
