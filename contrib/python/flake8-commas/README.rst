Flake8 Extension to enforce better comma placement.
===================================================

|Build Status| |PyPI - Version|

Usage
-----

If you are using flake8 it's as easy as:

.. code:: shell

    pip install flake8-commas

Now you can avoid those annoying merge conflicts on dictionary and list diffs.

Errors
------

Different versions of python require commas in different places. Ignore the
errors for languages you don't use in your flake8 config:

+------+-----------------------------------------+
| Code | message                                 |
+======+=========================================+
| C812 | missing trailing comma                  |
+------+-----------------------------------------+
| C813 | missing trailing comma in Python 3      |
+------+-----------------------------------------+
| C814 | missing trailing comma in Python 2      |
+------+-----------------------------------------+
| C815 | missing trailing comma in Python 3.5+   |
+------+-----------------------------------------+
| C816 | missing trailing comma in Python 3.6+   |
+------+-----------------------------------------+
| C818 | trailing comma on bare tuple prohibited |
+------+-----------------------------------------+
| C819 | trailing comma prohibited               |
+------+-----------------------------------------+

Examples
--------

.. code:: Python

    lookup_table = {
        'key1': 'value',
        'key2': 'something'  # <-- missing a trailing comma
    }

    json_data = json.dumps({
        "key": "value",
    }),                      # <-- incorrect trailing comma. json_data is now a tuple. Likely by accident.

Related tools
-------------

You may wish to consider one of the following tools alongside or instead of ``flake8-commas``.

For automatic insertion of commas, though noting that these tools may implement
different rules around where commas should be placed:

* `black <https://pypi.org/project/black/>`_, the uncompromising Python code formatter
* `add-trailing-comma <https://github.com/asottile/add-trailing-comma>`_, which can do comma insertion automatically


.. |Build Status| image:: https://github.com/PyCQA/flake8-commas/actions/workflows/.github/workflows/tests.yml/badge.svg?branch=main
   :target: https://github.com/PyCQA/flake8-commas/actions?query=branch%3Amain

.. |PyPI - Version| image:: https://img.shields.io/pypi/v/flake8-commas
   :target: https://pypi.org/project/flake8-commas/
