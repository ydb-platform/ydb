# coding: utf-8

from __future__ import print_function

import sys
import pytest  # NOQA

from .roundtrip import save_and_run  # NOQA


def test_monster(tmpdir):
    program_src = u'''\
    import srsly.ruamel_yaml
    from textwrap import dedent

    class Monster(srsly.ruamel_yaml.YAMLObject):
        yaml_tag = u'!Monster'

        def __init__(self, name, hp, ac, attacks):
            self.name = name
            self.hp = hp
            self.ac = ac
            self.attacks = attacks

        def __repr__(self):
            return "%s(name=%r, hp=%r, ac=%r, attacks=%r)" % (
                self.__class__.__name__, self.name, self.hp, self.ac, self.attacks)

    data = srsly.ruamel_yaml.load(dedent("""\\
        --- !Monster
        name: Cave spider
        hp: [2,6]    # 2d6
        ac: 16
        attacks: [BITE, HURT]
    """), Loader=srsly.ruamel_yaml.Loader)
    # normal dump, keys will be sorted
    assert srsly.ruamel_yaml.dump(data) == dedent("""\\
        !Monster
        ac: 16
        attacks: [BITE, HURT]
        hp: [2, 6]
        name: Cave spider
    """)
    '''
    assert save_and_run(program_src, tmpdir) == 1


@pytest.mark.skipif(sys.version_info < (3, 0), reason="no __qualname__")
def test_qualified_name00(tmpdir):
    """issue 214"""
    program_src = u"""\
    from srsly.ruamel_yaml import YAML
    from srsly.ruamel_yaml.compat import StringIO

    class A:
        def f(self):
            pass

    yaml = YAML(typ='unsafe', pure=True)
    yaml.explicit_end = True
    buf = StringIO()
    yaml.dump(A.f, buf)
    res = buf.getvalue()
    print('res', repr(res))
    assert res == "!!python/name:__main__.A.f ''\\n...\\n"
    x = yaml.load(res)
    assert x == A.f
    """
    assert save_and_run(program_src, tmpdir) == 1


@pytest.mark.skipif(sys.version_info < (3, 0), reason="no __qualname__")
def test_qualified_name01(tmpdir):
    """issue 214"""
    from srsly.ruamel_yaml import YAML
    import srsly.ruamel_yaml.comments
    from srsly.ruamel_yaml.compat import StringIO

    with pytest.raises(ValueError):
        yaml = YAML(typ="unsafe", pure=True)
        yaml.explicit_end = True
        buf = StringIO()
        yaml.dump(srsly.ruamel_yaml.comments.CommentedBase.yaml_anchor, buf)
        res = buf.getvalue()
        assert (
            res
            == "!!python/name:srsly.ruamel_yaml.comments.CommentedBase.yaml_anchor ''\n...\n"
        )
        x = yaml.load(res)
        assert x == srsly.ruamel_yaml.comments.CommentedBase.yaml_anchor
