"""\
PyJSON5\n\
=======\n\
\n\
A `JSON5 <https://spec.json5.org/>`_ serializer and parser library for Python 3 written in Cython.\n\
\n\
The serializer returns ASCII data that can safely be used in an HTML template.\n\
Apostrophes, ampersands, greater-than, and less-then signs are encoded as\n\
unicode escaped sequences. E.g. this snippet is safe for any and all input:\n\
\n\
.. code:: python\n\
\n\
    \"<a onclick='alert(\" + encode(data) + \")'>show message</a>\"\n\
\n\
Unless the input contains infinite or NaN values, the result will be valid\n\
JSON data.\n\
\n\
All valid JSON5 1.0.0 and `JSON <https://tools.ietf.org/html/rfc8259>`_ data can be read,\n\
unless the nesting level is absurdly high.\n\
"""
