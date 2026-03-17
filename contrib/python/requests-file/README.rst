Requests-File
=============

Requests-File is a transport adapter for use with the `Requests`_ Python
library to allow local filesystem access via file:\/\/ URLs.

To use:

.. code-block:: python

    import requests
    from requests_file import FileAdapter

    s = requests.Session()
    s.mount('file://', FileAdapter())

    resp = s.get('file:///path/to/file')

Features
--------

- Will open and read local files
- Might set a Content-Length header
- That's about it

No encoding information is set in the response object, so be careful using
Response.text: the chardet library will be used to convert the file to a
unicode type and it may not detect what you actually want.

EACCES is converted to a 403 status code, and ENOENT is converted to a
404. All other IOError types are converted to a 400.

Contributing
------------

Contributions welcome! Feel free to open a pull request against
https://codeberg.org/dashea/requests-file

License
-------

To maximise compatibility with Requests, this code is licensed under the Apache
license. See LICENSE for more details.

.. _`Requests`: https://github.com/psf/requests
