# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

from saltlint.linter.rule import DeprecationRule


class StateDeprecationDockerVolumeAbsent(DeprecationRule):
    id = '914'
    state = 'docker.volume_absent'
    deprecated_since = '2017.7.0'
    version_added = 'v0.5.0'
