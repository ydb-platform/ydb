# Copyright 2019-2024 Michael Samoglyadov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Formatters for Nested Diff."""

from html import escape as escape_html

import nested_diff
import nested_diff.handlers


class AbstractFormatter:
    """Base class for nested diff formatters."""

    default_generator = nested_diff.DEFAULT_HANDLER.generate_formatted_diff

    def __init__(
        self,
        *,
        handlers=None,
        indent='  ',
        line_separator='\n',
        sort_keys=True,
    ):
        """Initialize formatter.

        Args:
            handlers: Iterable with type handlers.
            indent: Prefix for each level of diff.
            line_separator: Text lines delimiter.
            sort_keys: Sort keys for dict-like structures.

        """
        self.indent = indent
        self.line_separator = line_separator
        self.sort_keys = sort_keys

        self.diff_prefix = ''
        self.diff_suffix = ''

        self.key_line_prefix = {
            'A': '+ ',
            'D': '  ',
            'N': '  ',
            'O': '  ',
            'R': '- ',
            'U': '  ',
        }

        self.key_prefix = dict.fromkeys(self.key_line_prefix, '')
        self.key_suffix = self.key_prefix.copy()

        self.val_line_prefix = self.key_line_prefix.copy()
        self.val_line_prefix['C'] = '# '
        self.val_line_prefix['E'] = '# '
        self.val_line_prefix['H'] = '  '
        self.val_line_prefix['O'] = '- '
        self.val_line_prefix['N'] = '+ '

        self.val_prefix = dict.fromkeys(self.val_line_prefix, '')
        self.val_suffix = self.val_prefix.copy()

        self.val_prefix['E'] = '<'
        self.val_suffix['E'] = '>'

        self.tags = (  # diff tags to format, sequence is important
            'D',
            'R',
            'O',
            'N',
            'A',
            'U',
        )

        self._gens_by_cls = {}
        self._gens_by_ext = {}
        self._type_by_ext = {}

        self.type_prefix = {}
        self.type_suffix = {}

        if handlers is None:
            handlers = (
                *nested_diff.TYPE_HANDLERS,
                nested_diff.handlers.TextHandler(),
            )

        for handler in handlers:
            self.set_handler(handler)

    def format(self, diff, **kwargs):
        """Return formatted diff as string."""
        return ''.join(self.generate_diff(diff, **kwargs))

    def set_handler(self, handler):
        """Set handler.

        Args:
            handler: instance of handlers.TypeHandler.

        """
        extension_id = handler.extension_id
        handled_type = handler.handled_type

        self._gens_by_cls[handled_type] = handler.generate_formatted_diff
        if extension_id is not None:
            self._gens_by_ext[extension_id] = handler.generate_formatted_diff

        self._type_by_ext[extension_id] = handled_type

        self.type_prefix[handled_type] = handler.type_prefix
        self.type_suffix[handled_type] = handler.type_suffix


class TextFormatter(AbstractFormatter):
    """Produce human friendly text diff with indenting formatting."""

    def __init__(self, *args, type_hints=True, **kwargs):
        """Initialize formatter.

        Args:
            args: Passed to base class as is.
            kwargs: Passed to base class as is.
            type_hints: Print values types when True.

        """
        super().__init__(*args, **kwargs)
        self.type_hints = type_hints

    def generate_diff(self, diff, depth=0):
        """Generate formatted diff."""
        yield self.diff_prefix

        try:
            extension_id = diff['E']
            try:
                generator = self._gens_by_ext[extension_id]
            except KeyError:
                raise ValueError(
                    f'unsupported extension: {extension_id}',
                ) from None
        except KeyError:
            extension_id = None
            try:
                generator = self._gens_by_cls[diff['D'].__class__]
            except KeyError:
                generator = self.default_generator

        try:
            comment = diff['C']
        except KeyError:
            if extension_id is not None and self.type_hints:
                yield from self.generate_string(
                    self._type_by_ext[extension_id].__name__,
                    'E',
                    depth,
                )
        else:
            for line in comment.splitlines():
                yield from self.generate_string(line, 'C', depth)

        yield from generator(self, diff, depth)

        yield self.diff_suffix

    def generate_key(self, key, tag, diff_type, depth):
        """Generate key line."""
        yield self.key_line_prefix[tag]
        yield self.indent * depth
        yield self.key_prefix[tag]
        yield self.type_prefix[diff_type]
        yield self.format_key(key)
        yield self.type_suffix[diff_type]
        yield self.key_suffix[tag]
        yield self.line_separator

    def generate_string(self, value, tag, depth):
        """Generate string line."""
        yield self.val_line_prefix[tag]
        yield self.indent * depth
        yield self.val_prefix[tag]
        yield self.format_string(value)
        yield self.val_suffix[tag]
        yield self.line_separator

    def generate_value(self, value, tag, depth):
        """Generate value line."""
        yield self.val_line_prefix[tag]
        yield self.indent * depth
        yield self.val_prefix[tag]
        yield self.format_value(value)
        yield self.val_suffix[tag]
        yield self.line_separator

    @staticmethod
    def get_diff_header(name_a, name_b):
        """Return diff header."""
        return f'--- {name_a}\n+++ {name_b}\n'

    @staticmethod
    def format_key(key):
        """Return key/index representation."""
        return key.__repr__()

    @staticmethod
    def format_string(val):
        """Return string representation."""
        return val

    @staticmethod
    def format_value(val):
        """Return value representation."""
        return val.__repr__()


class HtmlFormatter(TextFormatter):
    """Produce human friendly HTML diff with indenting formatting.

    Text copied from the browser should be exactly the same as TextFormatter
    produce.

    """

    def __init__(self, *args, **kwargs):
        """Initialize formatter.

        Args:
            args: Passed to base class as is.
            kwargs: Passed to base class as is.

        """
        super().__init__(*args, **kwargs)

        self.diff_prefix = '<div class="nDvD">'
        self.diff_suffix = '</div>'

        self.line_separator = '</div>'

        for key, val in self.key_line_prefix.items():
            self.key_line_prefix[key] = f'<div>{val}'
        for key, val in self.val_line_prefix.items():
            self.val_line_prefix[key] = f'<div>{val}'

        for key in self.key_line_prefix:
            self.key_prefix[key] = f'<div class="nDk{key}">'
            self.key_suffix[key] = '</div>'

        for key, val in self.val_prefix.items():
            self.val_prefix[key] = f'<div class="nDv{key}">{escape_html(val)}'

        for key, val in self.val_suffix.items():
            self.val_suffix[key] = f'{escape_html(val)}</div>'

    @staticmethod
    def get_css():
        """Return CSS for generated HTML page."""
        return (
            """
div:has(> [class^="nDk"]) {cursor: pointer}
div:has(> [class^="nDv"]) {cursor: text}
[class^="nDk"] {cursor: pointer}
[class^="nDk"], [class^="nDv"]:not(.nDvD) {
    border-radius: 2px;
    display: inline;
}
.nDkA {background-color: #cfc}
.nDkD, .nDkN, .nDkO {color: #000}
.nDkR {background-color: #fcc}
.nDkU, .nDvU {color: #777}
.nDvA, .nDvN {background-color: #dfd}
.nDvC, .nDvE {color: #00b}
.nDvD {
    display: block;
    font-family: monospace;
    overflow: hidden;
    transition-duration: .15s;
    transition-property: height;
    white-space: pre;
}
.nDvH {color: #707}
.nDvO, .nDvR {background-color: #fdd}
.nDvD div:not([class]):hover {background-color: rgba(0, 0, 0, .05)}
""".replace('    ', '')
            .replace(': ', ':')
            .replace(', ', ',')
            .replace(' {', '{')
            .replace('\n', '')
        )

    @staticmethod
    def get_diff_header(name_a, name_b):
        """Return diff header."""
        return f'<div class="nDvH">--- {name_a}<br>+++ {name_b}</div>'

    def get_page_footer(self):
        """Return HTML page footer."""
        return f'</div><script>{self.get_script()}</script></body></html>'

    def get_page_header(self, lang='en', title='Nested diff'):
        """Return HTML page header."""
        return (
            f'<!DOCTYPE html><html lang="{lang}"><head><title>'
            f'{escape_html(title)}</title><style>{self.get_css()}</style>'
            '</head><body><div class="nDvD" style="width:fit-content">'
        )

    def get_script(self):
        """Return script for generated HTML page."""
        script = """
var PREV = null;

document.querySelector('.nDvD').addEventListener('click', event => {
    tgt = event.target;

    if (tgt.className === '') {  // line div, no CSS class
        if (!tgt.firstElementChild.className.startsWith('nDk')) {
            return  // only key lines are togglers
        }
    } else if (tgt.className.startsWith('nDk')) {
        tgt = tgt.parentElement  // switch to line div
    } else {
        return  // only key lines are togglers
    }

    dif = tgt.nextSibling;  // diff is below the key line (nDvD div)

    if (!!PREV && !Object.is(PREV, dif)) {
        // restore height (captured value may, depends folded children or not)
        PREV.style.height = 'initial';
    }

    dif.style.height = dif.offsetHeight + 'px';

    if (dif.style.height === '0px') {
        dif.style.height = dif.__nDh + 'px';
        PREV = dif;
        tgt.innerHTML = tgt.innerHTML.replace(/^./, dif.__nDc);
        tgt.style.fontWeight = 'normal'
    } else {
        dif.__nDh = dif.offsetHeight;
        dif.style.height = '0px';
        PREV = null;
        dif.__nDc = tgt.innerHTML.substring(0, 1);
        tgt.innerHTML = tgt.innerHTML.replace(/^./, '&#9660;');
        tgt.style.fontWeight = 'bold'
    }
})
"""
        return ''.join(x.split('//', 1)[0].strip() for x in script.split('\n'))

    def format_key(self, key):
        """Return key/index representation."""
        return escape_html(super().format_key(key))

    def format_string(self, val):
        """Return string representation."""
        return escape_html(super().format_string(val))

    def format_value(self, val):
        """Return value representation."""
        return escape_html(super().format_value(val))


class TermFormatter(TextFormatter):
    """Same as TextFormatter but with term colors."""

    def __init__(self, *args, **kwargs):
        """Initialize formatter.

        Args:
            args: Passed to base class as is.
            kwargs: Passed to base class as is.

        """
        super().__init__(*args, **kwargs)

        self.line_separator = f'\x1b[0m{self.line_separator}'

        self.key_line_prefix['A'] = f'\x1b[1;32m{self.key_line_prefix["A"]}'
        self.key_line_prefix['R'] = f'\x1b[1;31m{self.key_line_prefix["R"]}'

        self.val_line_prefix['A'] = f'\x1b[32m{self.val_line_prefix["A"]}'
        self.val_line_prefix['C'] = f'\x1b[34m{self.val_line_prefix["C"]}'
        self.val_line_prefix['E'] = f'\x1b[34m{self.val_line_prefix["E"]}'
        self.val_line_prefix['H'] = f'\x1b[35m{self.val_line_prefix["H"]}'
        self.val_line_prefix['N'] = f'\x1b[32m{self.val_line_prefix["N"]}'
        self.val_line_prefix['O'] = f'\x1b[31m{self.val_line_prefix["O"]}'
        self.val_line_prefix['R'] = f'\x1b[31m{self.val_line_prefix["R"]}'

    @staticmethod
    def get_diff_header(name_a, name_b):
        """Return diff header."""
        return f'\x1b[33m--- {name_a}\n+++ {name_b}\x1b[0m\n'
