import typing as tp

import _pytest.mark.structures
import pytest


class ParameterSet():
    """
    The holder for a test variation, which preserves positional and keyword arguments.
    """
    test_name: str
    kwargs: dict[str, tp.Any]

    def __init__(self, test_name: str, /, **kwargs: tp.Any) -> None:
        self.test_name = test_name
        self.kwargs = kwargs


def safe_mark_parametrize(*param_sets: ParameterSet) -> pytest.MarkDecorator:
    """
    A wrapper around the @pytest.mark.parametrize(), which provides a safer interface.

    NOTE: Each ParameterSet object should be initialized with a single positional
          argument (which will be used as the name of the test variation)
          and any number of keyword arguments (which will be passed to the test
          when the given test variation is executed). This function enforces
          that all test variations have exactly the same set of keyword arguments.

    :param param_sets: The list of test variations to apply to the given test
    :type param_sets: ParameterSet

    :returns: The @pytest.mark.parametrize() configured accordingly
    :rtype: pytest.MarkDecorator
    """

    # Build the full set of test arguments
    all_arg_names: set[str] = set()

    for param in param_sets:
        all_arg_names.update(param.kwargs.keys())

    # Convert ParameterSet objects to the appropriate @pytest.mark.parametrize() arguments
    sorted_arg_names = sorted(all_arg_names)
    pytest_param_sets: list[_pytest.mark.structures.ParameterSet] = []

    for param in param_sets:
        assert set(param.kwargs.keys()) == all_arg_names, (
            "The test variation '{}' does not define all keyword arguments "
            "for the test, missing arguments: {}".format(
                param.test_name,
                ", ".join(sorted(all_arg_names - set(param.kwargs.keys()))),
            )
        )

        pytest_param_sets.append(
            pytest.param(
                *[param.kwargs[name] for name in sorted_arg_names],
                id=param.test_name,
            ),
        )

    return pytest.mark.parametrize(sorted_arg_names, pytest_param_sets)
