import re
from typing import List, Dict, Optional

"""

this file based on gdparsed by broaddeep
https://github.com/broaddeep/gdparser

"""


def extract_params(text: str) -> List[Dict[str, Optional[str]]]:
    """Extract parameters from args/kwargs section body text.

    Args:
        text (str): Args/Kwargs
        required (bool):
        javascript_type (bool): str -> string, int-> integer, float -> number, bool -> boolean

    Examples:
        [{'name': 'text',
         'type': 'str',
         'description': 'input text',
         'required': True,
         'enum': ['a', 'b']}]
    """

    pat = re.compile(r"\s*([A-Za-z_]+)[A-Za-z_\.\[\]\s\,\(\)]*:\s*")
    tmp = []
    for m in pat.finditer(text):
        s, e = m.span()
        name = m.group(1).strip()
        tmp.append((s, e, name, None))
        # print("name: %s, type: %s" % (name, type_))
    tmp.append((len(text), None, None, None))

    out = []
    for cur, nxt in zip(tmp, tmp[1:]):
        _, s, n, t = cur
        e, _, _, _ = nxt
        d = text[s:e].strip()
        o = {'name': n, 'type': t, 'description': d}
        out.append(o)
    return out


def clear_indent(text: str) -> str:
    lines = text.splitlines()
    effective_lines = []
    for _, line in enumerate(lines, 1):
        if len(line.replace(" ", "")) == 0:
            effective_lines.append((True, line))
        else:
            effective_lines.append((False, line))
    i = 0
    while True:
        try:
            chars = [line[i] for should_skip, line in effective_lines if not should_skip]
            can_remove_char = all(c.isspace() for c in chars)
            if not can_remove_char:
                raise IndexError
            else:
                i += 1
        except IndexError:
            newlines = [line if should_skip else line[i:] for should_skip, line in effective_lines]
            return "\n".join(newlines)


def parse_sections(text: str, supported_headers: Optional[List] = None) -> List[Dict[str, str]]:
    if supported_headers is None:
        supported_headers = [
            'Args',
            'Arguments',
            'Attention',
            'Attributes',
            'Caution',
            'Danger',
            'Error',
            'Example',
            'Examples',
            'Example Request',
            'Hint',
            'Important',
            'Keyword Args',
            'Keyword Arguments',
            'Kwargs',
            'Methods',
            'Note',
            'Notes',
            'Other Parameters',
            'Parameters',
            'Return',
            'Returns',
            'Raises',
            'References',
            'See Also',
            'Tip',
            'Todo',
            'Warning',
            'Warnings',
            'Warns',
            'Yield',
            'Yields',
        ]

    sh = "|".join(supported_headers)
    # in front of supported_headers, we should have at least one whitespace char (?<=\s)
    # any supported headers are concatenated to form regex group ({supported_headers})
    # after supported headers, zero or more white space, and colon can occur (\s*:)
    # after colon, return character should appear immediately (\n)

    pat = re.compile(r"(?<=\s)({supported_headers})\s*:\n".format(supported_headers=sh), flags=re.S)
    tmp = []
    tmp.append((None, 0, 'Overview'))
    for m in pat.finditer(text):
        s, e = m.span()
        section_header = m.group(0)[:-2]
        tmp.append((s, e, section_header))
    tmp.append((len(text), None, None))

    out = []
    for cur, nxt in zip(tmp, tmp[1:]):
        _, s, section_header = cur
        e, _, _ = nxt
        o = {'section_header': section_header, 'section_body': text[s:e]}
        out.append(o)
    return out


def parse_docstring(text: str, supported_headers=None, args_headers=None, kwargs_headers=None, remove_indent=True):
    """Parses Google-style Docstring.

    Args:
        text (str): docstring to parse written in Google-docstring format.

    Keyword Arguments:
        supported_headers (List[str] or None): list of text which can be recognized as the section headers,
            if None, the values are
            ['Args', 'Arguments', 'Attention', 'Attributes', 'Caution', 'Danger',
             'Error', 'Example', 'Examples', 'Example Request', 'Hint', 'Important', 'Keyword Args',
             'Keyword Arguments', 'Kwargs', 'Methods', 'Note', 'Notes', 'Other Parameters',
             'Parameters', 'Return', 'Returns', 'Raises', 'References', 'See Also',
             'Tip', 'Todo', 'Warning', 'Warnings', 'Warns', 'Yield', 'Yields']

        args_headers (List[str] or None): list of text which can be recognized as the argument section headers.
            Argument section headers is special which is parsed and fill the parameters of the final output.
            if None, the values are
            ['Args', 'Arguments', 'Parameters']

        kwargs_headers (List[str] or None): same as the args headers, except that the 'required' key of each parameter will become False.
            if None, the values are
            ['Kwargs', 'Keyword Args', 'Keyword Arguments']

        remove_indent (bool): whether indent is deleted or not. indent is defined as the
            common whitespace length across the lines within same section.

        javascript_type (bool): whether the python type notation should be converted into javascript type.
            currently following four types are converted.
            'str'- 'string', 'int'- 'integer', 'bool'- 'boolean', 'float'- 'number'
            if the type is not in the among the supported types, returned as it is without conversion.

    Returns:
        dict:
            - description (str) : function description
            - sections (List[Dict]) : the list of each section, each section is composed of two keys,
                 - section_header (str)
                 - section_body (str)
            - parameters (List[Dict]) : the list of each parameter, each parameter is composed of five keys,
                 - name (str): parameter name
                 - type (str): parameter type
                 - description (str): parameter description
                 - required (bool) : when the parameter is found in the Args section, it becomes True.
                      if found in Kwargs section, it become False
                 - enum (List or None) : You can make enum notation by using curly braces in the parameter description.
                      if the curly brace notation can be evaluated as set, the set value is used.
                      e.g. {'ab', 'cd'} - ['ab', 'cd']
                           {10, 20, 30} - [10, 20, 30]


    """
    raw_sections = parse_sections(text, supported_headers=supported_headers)

    args_headers = args_headers or ['Args', 'Arguments', 'Parameters']
    kwargs_headers = kwargs_headers or ['Kwargs', 'Keyword Args', 'Keyword Arguments']

    parameters, sections = [], []
    description = None
    for section in raw_sections:
        if remove_indent:
            section['section_body'] = clear_indent(section['section_body'])

        if section['section_header'] in args_headers + kwargs_headers:
            params = extract_params(section['section_body'])
            parameters.extend(params)

        elif section['section_header'] in ['Overview']:
            description = section['section_body']

        else:
            sections.append(section)

    return {'description': description, 'sections': sections, 'parameters': parameters}
