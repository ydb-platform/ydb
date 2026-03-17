# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

from saltlint.linter.rule import DeprecationRule


class StateDeprecationVirtUnpoweredRule(DeprecationRule):
    id = '905'
    state = 'virt.unpowered'
    deprecated_since = '2016.3.0'
    version_added = 'v0.5.0'
