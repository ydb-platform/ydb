import pytest

from typeguard._utils import function_name, qualified_name

from . import Child


@pytest.mark.parametrize(
    "inputval, add_class_prefix, expected",
    [
        pytest.param(qualified_name, False, "function", id="func"),
        pytest.param(Child(), False, "__tests__.Child", id="instance"),
        pytest.param(int, False, "int", id="builtintype"),
        pytest.param(int, True, "class int", id="builtintype_classprefix"),
    ],
)
def test_qualified_name(inputval, add_class_prefix, expected):
    assert qualified_name(inputval, add_class_prefix=add_class_prefix) == expected


def test_function_name():
    assert function_name(function_name) == "typeguard._utils.function_name"
