# coding: utf-8

from __future__ import print_function

"""
testing of anchors and the aliases referring to them
"""

import sys
import pytest


single_doc = """\
- a: 1
- b:
  - 2
  - 3
"""

single_data = [dict(a=1), dict(b=[2, 3])]

multi_doc = """\
---
- abc
- xyz
---
- a: 1
- b:
  - 2
  - 3
"""

multi_doc_data = [["abc", "xyz"], single_data]


def get_yaml():
    from srsly.ruamel_yaml import YAML

    return YAML()


class TestOldStyle:
    def test_single_load(self):
        d = get_yaml().load(single_doc)
        print(d)
        print(type(d[0]))
        assert d == single_data

    def test_single_load_no_arg(self):
        with pytest.raises(TypeError):
            assert get_yaml().load() == single_data

    def test_multi_load(self):
        data = list(get_yaml().load_all(multi_doc))
        assert data == multi_doc_data

    def test_single_dump(self, capsys):
        get_yaml().dump(single_data, sys.stdout)
        out, err = capsys.readouterr()
        assert out == single_doc

    def test_multi_dump(self, capsys):
        yaml = get_yaml()
        yaml.explicit_start = True
        yaml.dump_all(multi_doc_data, sys.stdout)
        out, err = capsys.readouterr()
        assert out == multi_doc


class TestContextManager:
    def test_single_dump(self, capsys):
        from srsly.ruamel_yaml import YAML

        with YAML(output=sys.stdout) as yaml:
            yaml.dump(single_data)
        out, err = capsys.readouterr()
        print(err)
        assert out == single_doc

    def test_multi_dump(self, capsys):
        from srsly.ruamel_yaml import YAML

        with YAML(output=sys.stdout) as yaml:
            yaml.explicit_start = True
            yaml.dump(multi_doc_data[0])
            yaml.dump(multi_doc_data[1])

        out, err = capsys.readouterr()
        print(err)
        assert out == multi_doc

    # input is not as simple with a context manager
    # you need to indicate what you expect hence load and load_all

    # @pytest.mark.xfail(strict=True)
    # def test_single_load(self):
    #     from srsly.ruamel_yaml import YAML
    #     with YAML(input=single_doc) as yaml:
    #         assert yaml.load() == single_data
    #
    # @pytest.mark.xfail(strict=True)
    # def test_multi_load(self):
    #     from srsly.ruamel_yaml import YAML
    #     with YAML(input=multi_doc) as yaml:
    #         for idx, data in enumerate(yaml.load()):
    #             assert data == multi_doc_data[0]

    def test_roundtrip(self, capsys):
        from srsly.ruamel_yaml import YAML

        with YAML(output=sys.stdout) as yaml:
            yaml.explicit_start = True
            for data in yaml.load_all(multi_doc):
                yaml.dump(data)

        out, err = capsys.readouterr()
        print(err)
        assert out == multi_doc
