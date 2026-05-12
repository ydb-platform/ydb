import sys
import warnings
from importlib import import_module
from importlib.util import cache_from_source
from pathlib import Path

import pytest

from typeguard import TypeCheckError, TypeguardFinder, install_import_hook
from typeguard._importhook import OPTIMIZATION

pytestmark = pytest.mark.filterwarnings("error:no type annotations present")
this_dir = Path(__file__).parent
dummy_module_path = this_dir / "dummymodule.py"
cached_module_path = Path(
    cache_from_source(str(dummy_module_path), optimization=OPTIMIZATION)
)


def import_dummymodule():
    if cached_module_path.exists():
        cached_module_path.unlink()

    sys.path.insert(0, str(this_dir))
    try:
        with install_import_hook(["dummymodule"]):
            with warnings.catch_warnings():
                warnings.filterwarnings("error", module="typeguard")
                module = import_module("dummymodule")
                return module
    finally:
        sys.path.remove(str(this_dir))


def test_blanket_import():
    dummymodule = import_dummymodule()
    try:
        pytest.raises(TypeCheckError, dummymodule.type_checked_func, 2, "3").match(
            r'argument "y" \(str\) is not an instance of int'
        )
    finally:
        del sys.modules["dummymodule"]


def test_package_name_matching():
    """
    The path finder only matches configured (sub)packages.
    """
    packages = ["ham", "spam.eggs"]
    dummy_original_pathfinder = None
    finder = TypeguardFinder(packages, dummy_original_pathfinder)

    assert finder.should_instrument("ham")
    assert finder.should_instrument("ham.eggs")
    assert finder.should_instrument("spam.eggs")

    assert not finder.should_instrument("spam")
    assert not finder.should_instrument("ha")
    assert not finder.should_instrument("spam_eggs")


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Requires ast.unparse()")
def test_debug_instrumentation(monkeypatch, capsys):
    monkeypatch.setattr("typeguard.config.debug_instrumentation", True)
    import_dummymodule()
    out, err = capsys.readouterr()
    path_str = str(dummy_module_path)
    # в ya make "path_str" разрешается в подкаталог ~/.ya/build/build_root/...
    assert f"{path_str!r} after instrumentation:"[1:] in err
    assert "class DummyClass" in err
