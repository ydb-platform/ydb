from __future__ import annotations

import pytest

from .utils import get_fixturenames_closure_and_arg2fixturedefs


def collect_fixtures(config: pytest.Config, items: list[pytest.Item]):
    fm = config.pluginmanager.get_plugin("funcmanage")
    func_items = [i for i in items if isinstance(i, pytest.Function)]
    for item in func_items:
        for marker in item.own_markers:
            if marker.name != "parametrize":
                continue
            params = marker.args[1] if len(marker.args) > 1 else marker.kwargs["argvalues"]
            arg2fixturedefs = {}
            for param in params:
                _, _arg2fixturedefs = get_fixturenames_closure_and_arg2fixturedefs(fm, item.parent, param)
                arg2fixturedefs.update(_arg2fixturedefs)
            item._fixtureinfo.name2fixturedefs.update(arg2fixturedefs)
