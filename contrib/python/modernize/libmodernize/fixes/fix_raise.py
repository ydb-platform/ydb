"""Fixer for 'raise E, V, T'

raise         -> raise
raise E       -> raise E
raise E, V    -> raise E(V)

raise (((E, E'), E''), E'''), V -> raise E(V)


CAVEATS:
1) "raise E, V" will be incorrectly translated if V is an exception
   instance. The correct Python 3 idiom is

        raise E from V

   but since we can't detect instance-hood by syntax alone and since
   any client code would have to be changed as well, we don't automate
   this.
"""
# Author: Collin Winter, Armin Ronacher
from __future__ import generator_stop

from fissix.fixes import fix_raise


class FixRaise(fix_raise.FixRaise):
    # We don't want to match 3-argument raise, with a traceback;
    # that is handled separately by fix_raise_six
    PATTERN = """
    raise_stmt< 'raise' exc=any [',' val=any] >
    """
