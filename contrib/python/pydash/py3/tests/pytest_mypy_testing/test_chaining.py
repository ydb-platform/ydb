import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_chain() -> None:
    reveal_type(_.chain([1, 2, 3, 4]).map(lambda x: x * 2).sum().value())  # R: builtins.int

    summer = _.chain([1, 2, 3, 4]).sum()
    reveal_type(summer)  # R: pydash.chaining.chaining.Chain[builtins.int]

    new_summer = summer.plant([1, 2])
    reveal_type(new_summer)  # R: pydash.chaining.chaining.Chain[builtins.int]

    reveal_type(new_summer.value())  # R: builtins.int
    reveal_type(summer.value())  # R: builtins.int

    def echo(item):
        print(item)

    summer = _.chain([1, 2, 3, 4]).for_each(echo).sum()
    committed = summer.commit()
    reveal_type(committed)  # R: pydash.chaining.chaining.Chain[builtins.int]

    reveal_type(committed.value())  # R: builtins.int
    reveal_type(summer.value())  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_tap() -> None:
    data = []

    def log(value):
        data.append(value)

    reveal_type(_.chain([1, 2, 3, 4]).map(lambda x: x * 2).tap(log).value())  # R: builtins.list[builtins.int]
