from typing import List, Generator, Tuple, Any, Dict, TypeVar, Type

from .typehints import Strlist

if False:  # pragma: nocover
    from .config import Section  # noqa

TypeFormatter = TypeVar('TypeFormatter', bound='FormatterBase')


def format_print_text(text: str, *, color_fg: str = None, color_bg: str = None) -> str:
    """Format given text using ANSI formatting escape sequences.

    Could be useful for print command.

    :param text:
    :param color_fg: text (foreground) color
    :param color_bg: text (background) color

    """
    from .config import Section

    color_fg = {

        'black': '30',
        'red': '31',
        'reder': '91',
        'green': '32',
        'greener': '92',
        'yellow': '33',
        'yellower': '93',
        'blue': '34',
        'bluer': '94',
        'magenta': '35',
        'magenter': '95',
        'cyan': '36',
        'cyaner': '96',
        'gray': '37',
        'grayer': '90',
        'white': '97',

    }.get(color_fg, '39')

    color_bg = {

        'black': '40',
        'red': '41',
        'reder': '101',
        'green': '42',
        'greener': '102',
        'yellow': '43',
        'yellower': '103',
        'blue': '44',
        'bluer': '104',
        'magenta': '45',
        'magenter': '105',
        'cyan': '46',
        'cyaner': '106',
        'gray': '47',
        'grayer': '100',
        'white': '107',

    }.get(color_bg, '49')

    mod = ';'.join([color_fg, color_bg])

    text = '%(esc)s[%(mod)sm%(value)s%(end)s' % {
        'esc': Section.vars.FORMAT_ESCAPE,
        'end': Section.vars.FORMAT_END,
        'mod': mod,
        'value': text,
    }

    return text


class FormatterBase:
    """Base class for configuration formatters."""

    alias: str = None

    def __init__(self, sections: List['Section']):
        self.sections = sections

    def iter_options(self) -> Generator[Tuple[str, str, Any], None, None]:
        """Iterates configuration sections groups options."""
        for section in self.sections:
            name = str(section)
            for key, value in section._get_options():
                yield name, key, value

    def format(self) -> Strlist:
        raise NotImplementedError()  # pragma: nocover


class IniFormatter(FormatterBase):
    """Translates a configuration as INI file."""

    alias: str = 'ini'

    def format(self) -> str:
        lines = []
        last_section = ''

        for section_name, key, value in self.iter_options():

            if section_name != last_section:
                lines.append(f'\n[{section_name}]')
                last_section = section_name

            lines.append(f'{key} = {str(value).strip()}')

        return '\n'.join(lines)


class ArgsFormatter(FormatterBase):
    """Translates a configuration to command line arguments."""

    alias: str = 'args'

    def format(self) -> List[str]:
        lines = []

        for section_name, key, value in self.iter_options():

            if section_name == 'uwsgi':
                value = str(value).strip()

                if value == 'true':
                    lines.append(f'--{key}')

                elif value.startswith('%') and len(value) == 2:
                    # No config var support is available in command line.
                    continue

                else:
                    lines.extend([f'--{key}', f'{value}'])

        return lines


FORMATTERS: Dict[str, Type[FormatterBase]] = {formatter.alias: formatter for formatter in (
    ArgsFormatter,
    IniFormatter,
)}
"""Available formatters by alias."""
