=========
deepmerge
=========

.. image:: https://github.com/toumorokoshi/deepmerge/actions/workflows/python-package.yaml/badge.svg
    :target: https://github.com/toumorokoshi/deepmerge/actions/workflows/python-package.yaml

A tools to handle merging of
nested data structures in python.

------------
Installation
------------

deepmerge is available on `pypi <https://pypi.python.org/>`_:

.. code-block:: bash

   pip install deepmerge

-------
Example
-------

**Generic Strategy**

.. code-block:: python

    from deepmerge import always_merger

    base = {"foo": ["bar"]}
    next = {"foo": ["baz"]}

    expected_result = {'foo': ['bar', 'baz']}
    result = always_merger.merge(base, next)

    assert expected_result == result


**Custom Strategy**

.. code-block:: python

    from deepmerge import Merger

    my_merger = Merger(
        # pass in a list of tuple, with the
        # strategies you are looking to apply
        # to each type.
        [
            (list, ["append"]),
            (dict, ["merge"]),
            (set, ["union"])
        ],
        # next, choose the fallback strategies,
        # applied to all other types:
        ["override"],
        # finally, choose the strategies in
        # the case where the types conflict:
        ["override"]
    )
    base = {"foo": ["bar"]}
    next = {"bar": "baz"}
    my_merger.merge(base, next)
    assert base == {"foo": ["bar"], "bar": "baz"}


You can also pass in your own merge functions, instead of a string.

For more information, see the `docs <https://deepmerge.readthedocs.io/en/latest/>`_