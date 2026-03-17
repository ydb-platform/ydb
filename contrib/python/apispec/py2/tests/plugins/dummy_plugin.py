# -*- coding: utf-8 -*-


def setup(spec):
    spec.old_plugins[
        '__tests__.plugins.dummy_plugin'
    ]['foo'] = 42
