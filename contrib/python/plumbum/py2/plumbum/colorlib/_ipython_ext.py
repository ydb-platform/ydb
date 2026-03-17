# -*- coding: utf-8 -*-
import sys

import IPython.display
from IPython.core.magic import Magics, cell_magic, magics_class, needs_local_scope

if sys.version_info >= (3,):
    from io import StringIO
else:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO  # type: ignore


valid_choices = [x[8:] for x in dir(IPython.display) if "display_" == x[:8]]


@magics_class
class OutputMagics(Magics):  # pragma: no cover
    @needs_local_scope
    @cell_magic
    def to(self, line, cell, local_ns=None):
        choice = line.strip()
        assert choice in valid_choices, "Valid choices for '%%to' are: " + str(
            valid_choices
        )
        display_fn = getattr(IPython.display, "display_" + choice)

        "Captures stdout and renders it in the notebook with some ."
        with StringIO() as out:
            old_out = sys.stdout
            try:
                sys.stdout = out
                exec(cell, self.shell.user_ns, local_ns)
                out.seek(0)
                display_fn(out.getvalue(), raw=True)
            finally:
                sys.stdout = old_out
