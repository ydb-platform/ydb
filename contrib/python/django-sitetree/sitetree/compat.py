from typing import Callable

try:
    from django.template.base import TokenType
    TOKEN_BLOCK = TokenType.BLOCK
    TOKEN_TEXT = TokenType.TEXT
    TOKEN_VAR = TokenType.VAR
except ImportError:
    from django.template.base import TOKEN_BLOCK, TOKEN_TEXT, TOKEN_VAR


class CommandOption:
    """Command line option wrapper."""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def options_getter(command_options):
    """Compatibility function to get rid of optparse in management commands after Django 1.10.

    :param tuple command_options: tuple with `CommandOption` objects.

    """
    def get_options(option_func: Callable = None):
        from optparse import make_option

        func = option_func or make_option
        options = tuple([func(*option.args, **option.kwargs) for option in command_options])

        return [] if option_func is None else options

    return get_options
