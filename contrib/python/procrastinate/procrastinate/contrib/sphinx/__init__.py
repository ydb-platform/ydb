from __future__ import annotations

from typing import Any

from sphinx.application import Sphinx
from sphinx.ext.autodoc import FunctionDocumenter

from procrastinate import tasks


class ProcrastinateTaskDocumenter(FunctionDocumenter):
    objtype = "procrastinate_task"
    directivetype = "function"
    member_order = 40

    @classmethod
    def can_document_member(
        cls,
        member: Any,
        membername: str,
        isattr: bool,
        parent: Any,
    ) -> bool:
        return isinstance(member, tasks.Task)


def setup(app: Sphinx):
    app.setup_extension("sphinx.ext.autodoc")  # Require autodoc extension

    app.add_autodocumenter(ProcrastinateTaskDocumenter)

    app.config.autodoc_use_legacy_class_based = True

    return {
        "version": "1",
        "parallel_read_safe": True,
    }
