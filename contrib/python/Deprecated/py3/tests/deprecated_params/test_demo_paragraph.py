# -*- coding: utf-8 -*-
"""
This example shows a function with a keyword-only parameter. A warning
message should be emitted if `color` is used (as a positional or keyword parameter).
"""
import sys
import warnings
import xml.sax.saxutils

import pytest

from deprecated.params import deprecated_params

PY3 = sys.version_info[0] == 3

if PY3:
    # Keyword-Only Arguments are only available for Python >= 3
    # On Python 2.7, this code raises a SyntaxError exception.
    exec(
        '''
@deprecated_params("color", reason="you should use 'styles' instead of 'color'")
def paragraph(text, *, color=None, styles=None):
    """Create a styled HTML paragraphe."""
    styles = styles or {}
    if color:
        styles['color'] = color
    html_styles = " ".join("{k}: {v};".format(k=k, v=v) for k, v in styles.items())
    html_text = xml.sax.saxutils.escape(text)
    fmt = '<p styles="{html_styles}">{html_text}</p>'
    return fmt.format(html_styles=html_styles, html_text=html_text)
        '''
    )

else:

    @deprecated_params("color", reason="you should use 'styles' instead of 'color'")
    def paragraph(text, color=None, styles=None):
        """Create a styled HTML paragraphe."""
        styles = styles or {}
        if color:
            styles['color'] = color
        html_styles = " ".join("{k}: {v};".format(k=k, v=v) for k, v in styles.items())
        html_text = xml.sax.saxutils.escape(text)
        fmt = '<p styles="{html_styles}">{html_text}</p>'
        return fmt.format(html_styles=html_styles, html_text=html_text)


@pytest.mark.parametrize(
    "args, kwargs, expected",
    [
        pytest.param(("Hello",), {}, [], id="'color' not used: no warnings"),
        pytest.param(("Hello",), {'styles': {'color': 'blue'}}, [], id="regular usage: no warnings"),
        pytest.param(
            ("Hello",),
            {'color': 'blue'},
            ["you should use 'styles' instead of 'color'"],
            id="'color' used in keyword-argument",
        ),
    ],
)
def test_paragraph(args, kwargs, expected):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        paragraph(*args, **kwargs)
    actual = [str(warn.message) for warn in warns]
    assert actual == expected
