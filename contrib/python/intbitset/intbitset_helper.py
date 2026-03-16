# This file is part of Invenio.
# Copyright (C) 2007, 2008, 2009, 2010, 2011, 2013, 2015, 2016 CERN.
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""
This function is needed only when load a pickled instanced of intbitset.
It needs to be stored in a real .py file, because currently cython
would have instantiate such a function as built-in function.
"""


def _(dump):
    """
    As part of the pickle protocol, a callable that will instantiate
    the class is needed.
    It's called _ just to make it short and hidden :-)
    """
    from intbitset import intbitset

    return intbitset(dump)
