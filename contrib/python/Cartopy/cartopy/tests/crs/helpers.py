# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Helpers for Cartopy CRS subclass tests.

"""


def check_proj_params(name, crs, other_args):
    expected = other_args | {f'proj={name}', 'no_defs'}
    proj_params = set(crs.proj4_init.lstrip('+').split(' +'))
    assert expected == proj_params
