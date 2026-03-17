from __future__ import annotations

import sys
from ast import AST
from contextlib import suppress
from tokenize import TokenInfo
from typing import TYPE_CHECKING, Any, Iterator, Sequence

from pylint.lint import Run
from pylint.reporters import BaseReporter


if TYPE_CHECKING:
    from pylint.message import Message
    from pylint.reporters.ureports.nodes import BaseLayout


STDIN = 'stdin'
PREFIX = 'PL'


def get_version() -> str:
    with suppress(ImportError):
        from pylint.__pkginfo__ import version  # type: ignore[attr-defined]
        return version
    with suppress(ImportError):
        from pylint.__pkginfo__ import __version__
        return __version__
    if sys.version_info >= (3, 8):
        from importlib import metadata as importlib_metadata
    else:
        import importlib_metadata
    try:
        return importlib_metadata.version('pylint')
    except Exception:
        pass
    return 'unknown'


class Reporter(BaseReporter):
    """PyLint reporter that memorizes all errors to be reported without printing them.
    """

    def __init__(self) -> None:
        self.errors: list[dict[str, Any]] = []
        super().__init__()

    def _display(self, layout: BaseLayout) -> None:
        pass

    def handle_message(self, msg: Message) -> None:
        # ignore `invalid syntax` messages, it is already checked by `pycodestyle`
        if msg.msg_id == 'E0001':
            return

        # flake8 stopped supporting 4-digit error codes,
        # so we drop the first digit of the code.
        # https://github.com/PyCQA/flake8/issues/1759
        code = msg.msg_id[0] + msg.msg_id[2:]
        assert len(code) == 4

        self.errors.append(dict(
            row=msg.line,
            col=msg.column,
            text='{prefix}{id} {msg} ({symbol})'.format(
                prefix=PREFIX,
                id=code,
                msg=msg.msg or '',
                symbol=msg.symbol,
            ),
            code=msg.msg_id,
        ))


class PyLintChecker:
    """The flake8 plugin entry point.
    """
    name = 'pylint'
    version = get_version()

    def __init__(
        self,
        tree: AST,
        file_tokens: Sequence[TokenInfo],
        filename: str = STDIN,
    ) -> None:
        self.tree = tree
        self.filename = filename

    def run(self) -> Iterator[tuple[int, int, str, type]]:
        reporter = Reporter()
        Run([self.filename], reporter=reporter, exit=False)
        for error in reporter.errors:
            yield error['row'], error['col'], error['text'], type(self)
