"""
This submodule is only an alias included for backwards compatibility. Its use is
deprecated as of 2.1.0.

Use `import arxiv`.
"""

from .__init__ import *  # noqa: F403
import warnings

warnings.warn("**Deprecated** after 2.1.0; use 'import arxiv' instead.")
