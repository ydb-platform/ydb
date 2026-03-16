# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

from saltlint.linter.rule import DeprecationRule


class StateDeprecationDockerNetworkPresent(DeprecationRule):
    id = '911'
    state = 'docker.network_present'
    deprecated_since = '2017.7.0'
    version_added = 'v0.5.0'
