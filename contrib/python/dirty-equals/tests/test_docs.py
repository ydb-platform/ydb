import platform
import sys
from pathlib import Path

import pytest
from pytest_examples import CodeExample, EvalExample, find_examples

root_dir = Path(__file__).parent.parent

examples = find_examples(
    root_dir / 'dirty_equals',
    root_dir / 'docs',
)


@pytest.mark.skipif(platform.python_implementation() == 'PyPy', reason='PyPy does not allow metaclass dunder methods')
@pytest.mark.skipif(sys.version_info >= (3, 12), reason="pytest-examples doesn't yet support 3.12")
@pytest.mark.parametrize('example', examples, ids=str)
def test_docstrings(example: CodeExample, eval_example: EvalExample):
    prefix_settings = example.prefix_settings()
    # E711 and E712 refer to `== True` and `== None` and need to be ignored
    # I001 refers is a problem with black and ruff disagreeing about blank lines :shrug:
    eval_example.set_config(ruff_ignore=['E711', 'E712', 'I001'])

    requires = prefix_settings.get('requires')
    if requires:
        requires_version = tuple(int(v) for v in requires.split('.'))
        if sys.version_info < requires_version:
            pytest.skip(f'requires python {requires}')

    if prefix_settings.get('test') != 'skip':
        if eval_example.update_examples:
            eval_example.run_print_update(example)
        else:
            eval_example.run_print_check(example)

    if prefix_settings.get('lint') != 'skip':
        if eval_example.update_examples:
            eval_example.format(example)
        else:
            eval_example.lint(example)
