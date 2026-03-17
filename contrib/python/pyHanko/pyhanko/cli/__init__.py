from pyhanko.cli._root import cli_root
from pyhanko.cli.commands.crypt import *
from pyhanko.cli.commands.fields import *
from pyhanko.cli.commands.signing import *
from pyhanko.cli.commands.stamp import *
from pyhanko.cli.commands.validation import *

__all__ = ['launch', 'cli_root']


def launch():
    cli_root(prog_name='pyhanko')
