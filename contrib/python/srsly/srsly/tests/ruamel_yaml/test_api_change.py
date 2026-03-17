# coding: utf-8

from __future__ import print_function

"""
testing of anchors and the aliases referring to them
"""

import sys
import textwrap
import pytest
from pathlib import Path

pytestmark = pytest.mark.filterwarnings(
    "ignore::pytest.PytestUnraisableExceptionWarning"
)


class TestNewAPI:
    def test_duplicate_keys_00(self):
        from srsly.ruamel_yaml import YAML
        from srsly.ruamel_yaml.constructor import DuplicateKeyError

        yaml = YAML()
        with pytest.raises(DuplicateKeyError):
            yaml.load("{a: 1, a: 2}")

    def test_duplicate_keys_01(self):
        from srsly.ruamel_yaml import YAML
        from srsly.ruamel_yaml.constructor import DuplicateKeyError

        yaml = YAML(typ="safe", pure=True)
        with pytest.raises(DuplicateKeyError):
            yaml.load("{a: 1, a: 2}")

    def test_duplicate_keys_02(self):
        from srsly.ruamel_yaml import YAML
        from srsly.ruamel_yaml.constructor import DuplicateKeyError

        yaml = YAML(typ="safe")
        with pytest.raises(DuplicateKeyError):
            yaml.load("{a: 1, a: 2}")

    def test_issue_135(self):
        # reported by Andrzej Ostrowski
        from srsly.ruamel_yaml import YAML

        data = {"a": 1, "b": 2}
        yaml = YAML(typ="safe")
        # originally on 2.7: with pytest.raises(TypeError):
        yaml.dump(data, sys.stdout)

    def test_issue_135_temporary_workaround(self):
        # never raised error
        from srsly.ruamel_yaml import YAML

        data = {"a": 1, "b": 2}
        yaml = YAML(typ="safe", pure=True)
        yaml.dump(data, sys.stdout)


class TestWrite:
    def test_dump_path(self, tmpdir):
        from srsly.ruamel_yaml import YAML

        fn = Path(str(tmpdir)) / "test.yaml"
        yaml = YAML()
        data = yaml.map()
        data["a"] = 1
        data["b"] = 2
        yaml.dump(data, fn)
        assert fn.read_text() == "a: 1\nb: 2\n"

    def test_dump_file(self, tmpdir):
        from srsly.ruamel_yaml import YAML

        fn = Path(str(tmpdir)) / "test.yaml"
        yaml = YAML()
        data = yaml.map()
        data["a"] = 1
        data["b"] = 2
        with open(str(fn), "w") as fp:
            yaml.dump(data, fp)
        assert fn.read_text() == "a: 1\nb: 2\n"

    def test_dump_missing_stream(self):
        from srsly.ruamel_yaml import YAML

        yaml = YAML()
        data = yaml.map()
        data["a"] = 1
        data["b"] = 2
        with pytest.raises(TypeError):
            yaml.dump(data)

    def test_dump_too_many_args(self, tmpdir):
        from srsly.ruamel_yaml import YAML

        fn = Path(str(tmpdir)) / "test.yaml"
        yaml = YAML()
        data = yaml.map()
        data["a"] = 1
        data["b"] = 2
        with pytest.raises(TypeError):
            yaml.dump(data, fn, True)

    def test_transform(self, tmpdir):
        from srsly.ruamel_yaml import YAML

        def tr(s):
            return s.replace(" ", "  ")

        fn = Path(str(tmpdir)) / "test.yaml"
        yaml = YAML()
        data = yaml.map()
        data["a"] = 1
        data["b"] = 2
        yaml.dump(data, fn, transform=tr)
        assert fn.read_text() == "a:  1\nb:  2\n"

    def test_print(self, capsys):
        from srsly.ruamel_yaml import YAML

        yaml = YAML()
        data = yaml.map()
        data["a"] = 1
        data["b"] = 2
        yaml.dump(data, sys.stdout)
        out, err = capsys.readouterr()
        assert out == "a: 1\nb: 2\n"


class TestRead:
    def test_multi_load(self):
        # make sure reader, scanner, parser get reset
        from srsly.ruamel_yaml import YAML

        yaml = YAML()
        yaml.load("a: 1")
        yaml.load("a: 1")  # did not work in 0.15.4

    def test_parse(self):
        # ensure `parse` method is functional and can parse "unsafe" yaml
        from srsly.ruamel_yaml import YAML
        from srsly.ruamel_yaml.constructor import ConstructorError

        yaml = YAML(typ="safe")
        s = "- !User0 {age: 18, name: Anthon}"
        # should fail to load
        with pytest.raises(ConstructorError):
            yaml.load(s)
        # should parse fine
        yaml = YAML(typ="safe")
        for _ in yaml.parse(s):
            pass


class TestLoadAll:
    def test_multi_document_load(self, tmpdir):
        """this went wrong on 3.7 because of StopIteration, PR 37 and Issue 211"""
        from srsly.ruamel_yaml import YAML

        fn = Path(str(tmpdir)) / "test.yaml"
        fn.write_text(
            textwrap.dedent(
                u"""\
            ---
            - a
            ---
            - b
            ...
            """
            )
        )
        yaml = YAML()
        assert list(yaml.load_all(fn)) == [["a"], ["b"]]


class TestDuplSet:
    def test_dupl_set_00(self):
        # round-trip-loader should except
        from srsly.ruamel_yaml import YAML
        from srsly.ruamel_yaml.constructor import DuplicateKeyError

        yaml = YAML()
        with pytest.raises(DuplicateKeyError):
            yaml.load(
                textwrap.dedent(
                    """\
                !!set
                ? a
                ? b
                ? c
                ? a
                """
                )
            )


class TestDumpLoadUnicode:
    # test triggered by SamH on stackoverflow (https://stackoverflow.com/q/45281596/1307905)
    # and answer by randomir (https://stackoverflow.com/a/45281922/1307905)
    def test_write_unicode(self, tmpdir):
        from srsly.ruamel_yaml import YAML

        yaml = YAML()
        text_dict = {"text": u"HELLO_WORLD©"}
        file_name = str(tmpdir) + "/tstFile.yaml"
        yaml.dump(text_dict, open(file_name, "w", encoding="utf8", newline="\n"))
        assert open(file_name, "rb").read().decode("utf-8") == u"text: HELLO_WORLD©\n"

    def test_read_unicode(self, tmpdir):
        from srsly.ruamel_yaml import YAML

        yaml = YAML()
        file_name = str(tmpdir) + "/tstFile.yaml"
        with open(file_name, "wb") as fp:
            fp.write(u"text: HELLO_WORLD©\n".encode("utf-8"))
        with open(file_name, "r", encoding="utf8") as fp:
            text_dict = yaml.load(fp)
        assert text_dict["text"] == u"HELLO_WORLD©"


class TestFlowStyle:
    def test_flow_style(self, capsys):
        # https://stackoverflow.com/questions/45791712/
        from srsly.ruamel_yaml import YAML

        yaml = YAML()
        yaml.default_flow_style = None
        data = yaml.map()
        data["b"] = 1
        data["a"] = [[1, 2], [3, 4]]
        yaml.dump(data, sys.stdout)
        out, err = capsys.readouterr()
        assert out == "b: 1\na:\n- [1, 2]\n- [3, 4]\n"


class TestOldAPI:
    @pytest.mark.skipif(sys.version_info >= (3, 0), reason="ok on Py3")
    def test_duplicate_keys_02(self):
        # Issue 165 unicode keys in error/warning
        from srsly.ruamel_yaml import safe_load
        from srsly.ruamel_yaml.constructor import DuplicateKeyError

        with pytest.raises(DuplicateKeyError):
            safe_load("type: Doméstica\ntype: International")
