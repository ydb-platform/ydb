from __future__ import annotations as _annotations

import platform
import re
import sys
from pathlib import Path

import pytest
from pytest_examples import CodeExample, EvalExample, find_examples
from pytest_examples.config import ExamplesConfig
from pytest_examples.lint import black_format

DOCS_ROOT = Path(__file__).parent.parent / 'docs'


def skip_docs_tests():
    if sys.platform not in {'linux', 'darwin'}:
        return 'not in linux or macos'

    if platform.python_implementation() != 'CPython':
        return 'not cpython'


class GroupModuleGlobals:
    def __init__(self) -> None:
        self.name = None
        self.module_dict: dict[str, str] = {}

    def get(self, name: str | None):
        if name is not None and name == self.name:
            return self.module_dict

    def set(self, name: str | None, module_dict: dict[str, str]):
        self.name = name
        if self.name is None:
            self.module_dict = None
        else:
            self.module_dict = module_dict


group_globals = GroupModuleGlobals()

skip_reason = skip_docs_tests()


def print_callback(print_statement: str) -> str:
    # make error display uniform
    s = re.sub(r'(https://errors.pydantic.dev)/.+?/', r'\1/2/', print_statement)
    # hack until https://github.com/pydantic/pytest-examples/issues/11 is fixed
    if '<built-in function cos>' in s:
        # avoid function repr breaking black formatting
        s = re.sub('<built-in function cos>', 'math.cos', s)
        return black_format(s, ExamplesConfig()).rstrip('\n')
    return s


@pytest.mark.filterwarnings('ignore:(parse_obj_as|schema_json_of|schema_of) is deprecated.*:DeprecationWarning')
@pytest.mark.skipif(bool(skip_reason), reason=skip_reason or 'not skipping')
@pytest.mark.parametrize('example', find_examples(str(DOCS_ROOT), skip=sys.platform == 'win32'), ids=str)
def test_docs_examples(  # noqa C901
    example: CodeExample, eval_example: EvalExample, tmp_path: Path, mocker, docs_test_env
):
    eval_example.print_callback = print_callback

    prefix_settings = example.prefix_settings()
    test_settings = prefix_settings.get('test')
    lint_settings = prefix_settings.get('lint')
    if test_settings == 'skip' and lint_settings == 'skip':
        pytest.skip('both test and lint skipped')

    requires_settings = prefix_settings.get('requires')
    if requires_settings:
        major, minor = map(int, requires_settings.split('.'))
        if sys.version_info < (major, minor):
            pytest.skip(f'requires python {requires_settings}')

    group_name = prefix_settings.get('group')

    if '# ignore-above' in example.source:
        eval_example.set_config(ruff_ignore=['E402'])
    if group_name:
        eval_example.set_config(ruff_ignore=['F821'])

    # eval_example.set_config(line_length=120)
    if lint_settings != 'skip':
        if eval_example.update_examples:
            eval_example.format(example)
        else:
            eval_example.lint(example)

    if test_settings == 'skip':
        return

    group_name = prefix_settings.get('group')
    d = group_globals.get(group_name)

    xfail = None
    if test_settings and test_settings.startswith('xfail'):
        xfail = test_settings[5:].lstrip(' -')

    rewrite_assertions = prefix_settings.get('rewrite_assert', 'true') == 'true'

    try:
        if test_settings == 'no-print-intercept':
            d2 = eval_example.run(example, module_globals=d, rewrite_assertions=rewrite_assertions)
        elif eval_example.update_examples:
            d2 = eval_example.run_print_update(example, module_globals=d, rewrite_assertions=rewrite_assertions)
        else:
            d2 = eval_example.run_print_check(example, module_globals=d, rewrite_assertions=rewrite_assertions)
    except BaseException as e:  # run_print_check raises a BaseException
        if xfail:
            pytest.xfail(f'{xfail}, {type(e).__name__}: {e}')
        raise
    else:
        if xfail:
            pytest.fail('expected xfail')
        group_globals.set(group_name, d2)
