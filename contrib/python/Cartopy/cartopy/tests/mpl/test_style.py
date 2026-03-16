# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import warnings

import pytest


with warnings.catch_warnings():
    warnings.simplefilter('ignore', DeprecationWarning)
    from cartopy.mpl import style


@pytest.mark.parametrize(
    ('styles', 'expected'),
    [([], {}),
     ([{}, {}, {}], {}),
     ([{}, dict(a=2), dict(a=1)], dict(a=1)),
     ([dict(fc='red')], dict(facecolor='red')),
     ([dict(fc='red', color='blue')],
      dict(facecolor='blue', edgecolor='blue')),
     ([dict(fc='red', facecolor='blue')], dict(facecolor='blue')),
     ([dict(color='red')],
      dict(edgecolor='red', facecolor='red')),
     ([dict(edgecolor='blue'), dict(color='red')],
      dict(edgecolor='red', facecolor='red')),
     ([dict(edgecolor='blue'), dict(color='red')],
      dict(edgecolor='red', facecolor='red')),
     ([dict(color='blue'), dict(edgecolor='red')],
      dict(edgecolor='red', facecolor='blue')),
     # Even if you set an edgecolor, color should trump it.
     ([dict(color='blue'), dict(edgecolor='red', color='yellow')],
      dict(edgecolor='yellow', facecolor='yellow')),
     # Support for 'never' being honoured.
     ([dict(facecolor='never'), dict(color='yellow')],
      dict(edgecolor='yellow', facecolor='never')),
     ([dict(lw=1, linewidth=2)], dict(linewidth=2)),
     ([dict(lw=1, linewidth=2), dict(lw=3)], dict(linewidth=3)),
     ([dict(color=None), dict(facecolor='red')],
      dict(facecolor='red', edgecolor=None)),
     ([dict(linewidth=1), dict(lw=None)], dict(linewidth=None)),
     ([dict(facecolor='never'), dict(fc='NoNe')], dict(facecolor='never')),
     ]
)
def test_merge(styles, expected):
    merged_style = style.merge(*styles)
    assert merged_style == expected


@pytest.mark.parametrize("case", [{'fc': 'red'}, {'fc': 1}])
def test_merge_warning(case):
    with pytest.warns(UserWarning, match=r'defined as \"never\"'):
        style.merge({'facecolor': 'never'}, case)


@pytest.mark.parametrize(
    ('style_d', 'expected'),
    [
        # Support for 'never' being honoured.
        (dict(facecolor='never', edgecolor='yellow'),
         dict(edgecolor='yellow', facecolor='none')),
    ])
def test_finalize(style_d, expected):
    assert style.finalize(style_d) == expected
    # Double check we are updating in-place
    assert style_d == expected
