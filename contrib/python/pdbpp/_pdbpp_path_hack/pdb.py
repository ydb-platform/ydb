"""Hijack the stdlib pdb module on import.

This gets inserted to the beginning of sys.path via pdbpp_hijack_pdb.pth.

You can set PDBPP_HIJACK_PDB=0 as an environment variable to skip it.
"""

import os
import sys

pdb_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pdbpp.py")

# Set __file__ to exec'd code.  This is good in general, and required for
# coverage.py to use it.
__file__ = pdb_path

with open(pdb_path) as f:
    exec(compile(f.read(), pdb_path, "exec"))

# Update/set __file__ attribute to actually sourced file, but not when not coming
# here via "-m pdb", where __name__ is "__main__".
if __name__ != "__main__":
    sys.modules["pdb"].__file__ = pdb_path
