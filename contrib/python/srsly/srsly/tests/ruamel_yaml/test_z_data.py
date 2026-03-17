# coding: utf-8

from __future__ import print_function, unicode_literals

import sys
import pytest  # NOQA
import warnings  # NOQA

from pathlib import Path

base_path = Path("data")  # that is srsly.ruamel_yaml.data
PY2 = sys.version_info[0] == 2


class YAMLData(object):
    yaml_tag = "!YAML"

    def __init__(self, s):
        self._s = s

    # Conversion tables for input. E.g. "<TAB>" is replaced by "\t"
    # fmt: off
    special = {
        'SPC': ' ',
        'TAB': '\t',
        '---': '---',
        '...': '...',
    }
    # fmt: on

    @property
    def value(self):
        if hasattr(self, "_p"):
            return self._p
        assert " \n" not in self._s
        assert "\t\n" not in self._s
        self._p = self._s
        for k, v in YAMLData.special.items():
            k = "<" + k + ">"
            self._p = self._p.replace(k, v)
        return self._p

    def test_rewrite(self, s):
        assert " \n" not in s
        assert "\t\n" not in s
        for k, v in YAMLData.special.items():
            k = "<" + k + ">"
            s = s.replace(k, v)
        return s

    @classmethod
    def from_yaml(cls, constructor, node):
        from srsly.ruamel_yaml.nodes import MappingNode

        if isinstance(node, MappingNode):
            return cls(constructor.construct_mapping(node))
        return cls(node.value)


class Python(YAMLData):
    yaml_tag = "!Python"


class Output(YAMLData):
    yaml_tag = "!Output"


class Assert(YAMLData):
    yaml_tag = "!Assert"

    @property
    def value(self):
        from srsly.ruamel_yaml.compat import Mapping

        if hasattr(self, "_pa"):
            return self._pa
        if isinstance(self._s, Mapping):
            self._s["lines"] = self.test_rewrite(self._s["lines"])
        self._pa = self._s
        return self._pa


def pytest_generate_tests(metafunc):
    test_yaml = []
    paths = sorted(base_path.glob("**/*.yaml"))
    idlist = []
    for path in paths:
        stem = path.stem
        if stem.startswith(".#"):  # skip emacs temporary file
            continue
        idlist.append(stem)
        test_yaml.append([path])
    metafunc.parametrize(["yaml"], test_yaml, ids=idlist, scope="class")


class TestYAMLData(object):
    def yaml(self, yaml_version=None):
        from srsly.ruamel_yaml import YAML

        y = YAML()
        y.preserve_quotes = True
        if yaml_version:
            y.version = yaml_version
        return y

    def docs(self, path):
        from srsly.ruamel_yaml import YAML

        tyaml = YAML(typ="safe", pure=True)
        tyaml.register_class(YAMLData)
        tyaml.register_class(Python)
        tyaml.register_class(Output)
        tyaml.register_class(Assert)
        return list(tyaml.load_all(path))

    def yaml_load(self, value, yaml_version=None):
        yaml = self.yaml(yaml_version=yaml_version)
        data = yaml.load(value)
        return yaml, data

    def round_trip(self, input, output=None, yaml_version=None):
        from srsly.ruamel_yaml.compat import StringIO

        yaml, data = self.yaml_load(input.value, yaml_version=yaml_version)
        buf = StringIO()
        yaml.dump(data, buf)
        expected = input.value if output is None else output.value
        value = buf.getvalue()
        if PY2:
            value = value.decode("utf-8")
            print("value", value)
            # print('expected', expected)
        assert value == expected

    def load_assert(self, input, confirm, yaml_version=None):
        from srsly.ruamel_yaml.compat import Mapping

        d = self.yaml_load(input.value, yaml_version=yaml_version)[1]  # NOQA
        print("confirm.value", confirm.value, type(confirm.value))
        if isinstance(confirm.value, Mapping):
            r = range(confirm.value["range"])
            lines = confirm.value["lines"].splitlines()
            for idx in r:  # NOQA
                for line in lines:
                    line = "assert " + line
                    print(line)
                    exec(line)
        else:
            for line in confirm.value.splitlines():
                line = "assert " + line
                print(line)
                exec(line)

    def run_python(self, python, data, tmpdir):
        from .roundtrip import save_and_run

        assert save_and_run(python.value, base_dir=tmpdir, output=data.value) == 0

    # this is executed by pytest the methods with names not starting with test_
    # are helpers
    def test_yaml_data(self, yaml, tmpdir):
        from srsly.ruamel_yaml.compat import Mapping

        idx = 0
        typ = None
        yaml_version = None

        docs = self.docs(yaml)
        if isinstance(docs[0], Mapping):
            d = docs[0]
            typ = d.get("type")
            yaml_version = d.get("yaml_version")
            if "python" in d:
                if not check_python_version(d["python"]):
                    pytest.skip("unsupported version")
            idx += 1
        data = output = confirm = python = None
        for doc in docs[idx:]:
            if isinstance(doc, Output):
                output = doc
            elif isinstance(doc, Assert):
                confirm = doc
            elif isinstance(doc, Python):
                python = doc
                if typ is None:
                    typ = "python_run"
            elif isinstance(doc, YAMLData):
                data = doc
            else:
                print("no handler for type:", type(doc), repr(doc))
                raise AssertionError()
        if typ is None:
            if data is not None and output is not None:
                typ = "rt"
            elif data is not None and confirm is not None:
                typ = "load_assert"
            else:
                assert data is not None
                typ = "rt"
        print("type:", typ)
        if data is not None:
            print("data:", data.value, end="")
        print("output:", output.value if output is not None else output)
        if typ == "rt":
            self.round_trip(data, output, yaml_version=yaml_version)
        elif typ == "python_run":
            self.run_python(python, output if output is not None else data, tmpdir)
        elif typ == "load_assert":
            self.load_assert(data, confirm, yaml_version=yaml_version)
        else:
            print("\nrun type unknown:", typ)
            raise AssertionError()


def check_python_version(match, current=None):
    """
    version indication, return True if version matches.
    match should be something like 3.6+, or [2.7, 3.3] etc. Floats
    are converted to strings. Single values are made into lists.
    """
    if current is None:
        current = list(sys.version_info[:3])
    if not isinstance(match, list):
        match = [match]
    for m in match:
        minimal = False
        if isinstance(m, float):
            m = str(m)
        if m.endswith("+"):
            minimal = True
            m = m[:-1]
        # assert m[0].isdigit()
        # assert m[-1].isdigit()
        m = [int(x) for x in m.split(".")]
        current_len = current[: len(m)]
        # print(m, current, current_len)
        if minimal:
            if current_len >= m:
                return True
        else:
            if current_len == m:
                return True
    return False
