# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

import six


def quote_with_encoding(val):
    """Quote a string that will be placed in url.
    If the string is unicode, we encode it
    to utf-8 before using `urllib.parse.quote`.
    In case it's not a string (an int for instance),
    we still try to convert it.

    Args:
            val: The string to be properly encoded.
    """
    if not isinstance(val, (six.integer_types, six.string_types)):
        raise ValueError("Cannot encode {} type.".format(type(val)))

    # handle other stuff than strings
    if isinstance(val, six.integer_types):
        val = six.text_type(val).encode('utf-8') if six.PY3 else bytes(val)

    # works with PY2 and PY3
    elif not isinstance(val, bytes):
        val = val.encode("utf-8")

    return six.moves.urllib.parse.quote(val)
