import json
import os

import res


class FakeUnit:
    def __init__(self, source_root):
        self.source_root = source_root
        self.resource_files = []

    def resolve(self, path):
        assert path.startswith('$S/')
        return os.path.join(self.source_root, path[len('$S/') :])

    def onresource_files(self, args):
        self.resource_files.append(args)


def touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def test_ya_tools_conf_collects_resource_files(tmp_path):
    conf_dir = tmp_path / 'conf'
    write_json(
        conf_dir / 'ya.conf.json',
        {
            'bottles': {
                'shared': {'formula': 'conf/formulas/root.json'},
                'legacy': {'formula': 'conf/formulas/legacy.json'},
                'object': {'formula': {'resource': 'ignored'}},
            }
        },
    )
    touch(conf_dir / 'tools' / 'internal' / 'tiers.json')
    write_json(
        conf_dir / 'tools' / 'tools' / 'root.tool.json',
        {'tool': {'type': 'simple', 'definition': {'formula': 'conf/formulas/root.json'}}},
    )
    write_json(
        conf_dir / 'tools' / 'tools' / 'parent' / 'child.tool.json',
        {'tool': {'type': 'simple', 'definition': {'formula': 'conf/formulas/tool.json'}}},
    )
    write_json(
        conf_dir / 'tools' / 'tools' / 'default.tool.json',
        {'tool': {'type': 'simple', 'definition': {}}},
    )
    write_json(
        conf_dir / 'tools' / 'tools' / 'parent' / 'default-child.tool.json',
        {'tool': {'type': 'simple'}},
    )
    write_json(
        conf_dir / 'tools' / 'tools' / 'object.tool.json',
        {'tool': {'type': 'simple', 'definition': {'formula': {'resource': 'ignored'}}}},
    )
    write_json(
        conf_dir / 'tools' / 'tools' / 'parent.tool.json',
        {'tool': {'type': 'parent', 'definition': {'formula': 'outside/ignored.json'}}},
    )
    touch(conf_dir / 'tools' / 'tools' / 'parent' / 'ignored.json')
    write_json(
        conf_dir / 'tools' / 'toolchains' / 'default.toolchain.json',
        {
            'bottles': {
                'shared': {'formula': 'conf/formulas/root.json'},
                'toolchain': {'formula': 'conf/formulas/toolchain.json'},
                'object': {'formula': {'resource': 'ignored'}},
            }
        },
    )
    touch(conf_dir / 'tools' / 'toolchains' / 'ignored.json')
    touch(conf_dir / 'tools' / 'toolchains' / 'nested' / 'ignored.toolchain.json')
    touch(conf_dir / 'formulas' / 'root.json')
    touch(conf_dir / 'formulas' / 'legacy.json')
    touch(conf_dir / 'formulas' / 'tool.json')
    touch(conf_dir / 'formulas' / 'toolchain.json')
    touch(tmp_path / 'build' / 'external_resources' / 'default' / 'resources.json')
    touch(tmp_path / 'build' / 'external_resources' / 'parent' / 'default-child' / 'resources.json')

    unit = FakeUnit(str(tmp_path))
    res._YA_TOOLS_CONF(unit, 'conf/')

    assert unit.resource_files == [
        ['STRIP', 'conf/', 'conf/ya.conf.json'],
        [
            'PREFIX',
            'yatools',
            'STRIP',
            'conf',
            'conf/tools/internal/tiers.json',
            'conf/tools/toolchains/default.toolchain.json',
            'conf/tools/tools/default.tool.json',
            'conf/tools/tools/object.tool.json',
            'conf/tools/tools/parent.tool.json',
            'conf/tools/tools/parent/child.tool.json',
            'conf/tools/tools/parent/default-child.tool.json',
            'conf/tools/tools/root.tool.json',
        ],
        'build/external_resources/default/resources.json',
        'build/external_resources/parent/default-child/resources.json',
        'conf/formulas/legacy.json',
        'conf/formulas/root.json',
        'conf/formulas/tool.json',
        'conf/formulas/toolchain.json',
    ]


def test_ya_tools_conf_allows_missing_optional_directories(tmp_path):
    write_json(tmp_path / 'conf' / 'ya.conf.json', {'bottles': {}})
    unit = FakeUnit(str(tmp_path))

    res._YA_TOOLS_CONF(unit, 'conf')

    assert unit.resource_files == [['STRIP', 'conf/', 'conf/ya.conf.json']]


def test_ya_tools_conf_reports_missing_config_directory(tmp_path, monkeypatch):
    errors = []
    monkeypatch.setattr(res.ymake, 'report_configure_error', errors.append, raising=False)
    unit = FakeUnit(str(tmp_path))

    res._YA_TOOLS_CONF(unit, 'missing')

    assert unit.resource_files == []
    assert errors == ['Directory "{}" not found'.format(tmp_path / 'missing')]


def test_ya_tools_conf_reports_missing_ya_conf_json(tmp_path, monkeypatch):
    (tmp_path / 'conf').mkdir()
    errors = []
    monkeypatch.setattr(res.ymake, 'report_configure_error', errors.append, raising=False)
    unit = FakeUnit(str(tmp_path))

    res._YA_TOOLS_CONF(unit, 'conf')

    assert unit.resource_files == []
    assert errors == ['File "{}" not found'.format(tmp_path / 'conf' / 'ya.conf.json')]


def test_ya_tools_conf_reports_invalid_formulas(tmp_path, monkeypatch):
    conf_dir = tmp_path / 'conf'
    write_json(conf_dir / 'ya.conf.json', {'bottles': {}})
    write_json(
        conf_dir / 'tools' / 'tools' / 'outside.tool.json',
        {'tool': {'type': 'simple', 'definition': {'formula': 'conf-other/formula.json'}}},
    )
    write_json(
        conf_dir / 'tools' / 'tools' / 'missing.tool.json',
        {'tool': {'type': 'simple', 'definition': {'formula': 'conf/formulas/missing.json'}}},
    )
    errors = []
    monkeypatch.setattr(res.ymake, 'report_configure_error', errors.append, raising=False)
    unit = FakeUnit(str(tmp_path))

    res._YA_TOOLS_CONF(unit, 'conf')

    assert len(errors) == 2
    assert 'must be located in "build" or "conf" file tree' in errors[1]
    assert 'conf/formulas/missing.json' in errors[0]
