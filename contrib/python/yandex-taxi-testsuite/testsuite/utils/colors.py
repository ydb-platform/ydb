import sys


class Colors:
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    GRAY = '\033[37m'
    DARK_GRAY = '\033[37m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    DEFAULT = '\033[0m'
    DEFAULT_BG = '\033[49m'
    BG_BLACK = '\033[40m'


def should_enable_color(pytestconfig) -> bool:
    option = getattr(pytestconfig.option, 'color', 'no')
    if option == 'yes':
        return True
    if option == 'auto':
        return sys.stderr.isatty()
    return False
