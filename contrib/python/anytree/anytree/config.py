"""Central Configuration."""

import os

# Global Option which enables all internal assertions.
ASSERTIONS = bool(int(os.environ.get("ANYTREE_ASSERTIONS", "0")))
