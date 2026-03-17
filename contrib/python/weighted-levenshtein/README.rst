weighted-levenshtein
====================

.. image:: https://circleci.com/gh/infoscout/weighted-levenshtein.svg?style=svg
    :target: https://circleci.com/gh/infoscout/weighted-levenshtein

.. image:: https://coveralls.io/repos/github/infoscout/weighted-levenshtein/badge.svg?branch=master
    :target: https://coveralls.io/github/infoscout/weighted-levenshtein?branch=master

Use Cases
---------

Most existing Levenshtein libraries are not very flexible: all edit operations have cost 1.

However, sometimes not all edits are created equal. For instance, if you are doing OCR correction, maybe substituting '0' for 'O' should have a smaller cost than substituting 'X' for 'O'. If you are doing human typo correction, maybe substituting 'X' for 'Z' should have a smaller cost, since they are located next to each other on a QWERTY keyboard.

This library supports all theses use cases, by allowing the user to specify different weights for edit operations involving every possible combination of letters. The core algorithms are written in Cython, which means they are blazing fast to run.

The Levenshtein distance function supports setting different costs for inserting characters, deleting characters, and substituting characters. Thus, Levenshtein distance is well suited for detecting OCR errors.

The Damerau-Levenshtein distance function supports setting different costs for inserting characters, deleting characters, substituting characters, and transposing characters. Thus, Damerau-Levenshtein distance is well suited for detecting human typos, since humans are likely to make transposition errors, while OCR is not.

More Information
----------------

Levenshtein distance:
https://en.wikipedia.org/wiki/Levenshtein\_distance and
https://en.wikipedia.org/wiki/Wagner%E2%80%93Fischer\_algorithm

Optimal String Alignment:
https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein\_distance#Optimal\_string\_alignment\_distance

Damerau-Levenshtein distance:
https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein\_distance#Distance\_with\_adjacent\_transpositions



Installation
------------

``pip install weighted-levenshtein``

Usage Example
-------------

.. code:: python

    import numpy as np
    from weighted_levenshtein import lev, osa, dam_lev


    insert_costs = np.ones(128, dtype=np.float64)  # make an array of all 1's of size 128, the number of ASCII characters
    insert_costs[ord('D')] = 1.5  # make inserting the character 'D' have cost 1.5 (instead of 1)

    # you can just specify the insertion costs
    # delete_costs and substitute_costs default to 1 for all characters if unspecified
    print(lev('BANANAS', 'BANDANAS', insert_costs=insert_costs))  # prints '1.5'

    delete_costs = np.ones(128, dtype=np.float64)
    delete_costs[ord('S')] = 0.5  # make deleting the character 'S' have cost 0.5 (instead of 1)

    # or you can specify both insertion and deletion costs (though in this case insertion costs don't matter)
    print(lev('BANANAS', 'BANANA', insert_costs=insert_costs, delete_costs=delete_costs))  # prints '0.5'


    substitute_costs = np.ones((128, 128), dtype=np.float64)  # make a 2D array of 1's
    substitute_costs[ord('H'), ord('B')] = 1.25  # make substituting 'H' for 'B' cost 1.25

    print(lev('HANANA', 'BANANA', substitute_costs=substitute_costs))  # prints '1.25'

    # it's not symmetrical! in this case, it is substituting 'B' for 'H'
    print(lev('BANANA', 'HANANA', substitute_costs=substitute_costs))  # prints '1'

    # to make it symmetrical, you need to set both costs in the 2D array
    substitute_costs[ord('B'), ord('H')] = 1.25  # make substituting 'B' for 'H' cost 1.25 as well

    print(lev('BANANA', 'HANANA', substitute_costs=substitute_costs))  # now it prints '1.25'


    transpose_costs = np.ones((128, 128), dtype=np.float64)
    transpose_costs[ord('A'), ord('B')] = 0.75  # make swapping 'A' for 'B' cost 0.75

    # note: now using dam_lev. lev does not support swapping, but osa and dam_lev do.
    # See Wikipedia links for difference between osa and dam_lev
    print(dam_lev('ABNANA', 'BANANA', transpose_costs=transpose_costs))  # prints '0.75'

    # like substitution, transposition is not symmetrical either!
    print(dam_lev('BANANA', 'ABNANA', transpose_costs=transpose_costs))  # prints '1'

    # you need to explicitly set the other direction as well
    transpose_costs[ord('B'), ord('A')] = 0.75  # make swapping 'B' for 'A' cost 0.75

    print(dam_lev('BANANA', 'ABNANA', transpose_costs=transpose_costs))  # now it prints '0.75'


``lev``, ``osa``, and ``dam_lev`` are aliases for ``levenshtein``,
``optimal_string_alignment``, and ``damerau_levenshtein``, respectively.

Detailed Documentation
----------------------

http://weighted-levenshtein.readthedocs.io/

Important Notes
---------------

- All string lookups are case sensitive.

- The costs parameters only accept numpy arrays, since the underlying Cython implementation relies on this for fast lookups. The numpy arrays are indexed using the ``ord()`` value of the characters. Thus, only the first 128 ASCII letters are accepted, and ``dict`` and ``list`` are not accepted. Consequently, the strings must be strictly ``str`` objects, not ``unicode``.

- This library is compatible with both Python 2 and Python 3 (see ``tox.ini`` for tested versions).



Use as Cython library
---------------------

.. code:: cython

    from weighted_levenshtein.clev cimport c_levenshtein as lev, c_optimal_string_alignment as osa, c_damerau_levenshtein as dam_lev
    import numpy as np

    a = np.ones(128, dtype=np.float64)
    b = np.ones((128, 128), dtype=np.float64)

    print(lev("BANANA", 4, "BANANAS", 5, a, a, b))

For the Cython API, functions are prefixed with a ``c_`` with respect to the Python API. Also, the string parameters are followed by their length. The data types of the numpy arrays specifying the costs still need to be ``np.float64``, consistent with the Python API.


Function signatures below:

.. code:: cython

    cdef double c_damerau_levenshtein(
        unsigned char* str_a,
        Py_ssize_t len_a,
        unsigned char* str_b,
        Py_ssize_t len_b,
        double[::1] insert_costs,
        double[::1] delete_costs,
        double[:,::1] substitute_costs,
        double[:,::1] transpose_costs) nogil


    cdef double c_optimal_string_alignment(
        unsigned char* word_m,
        Py_ssize_t m,
        unsigned char* word_n,
        Py_ssize_t n,
        double[::1] insert_costs,
        double[::1] delete_costs,
        double[:,::1] substitute_costs,
        double[:,::1] transpose_costs) nogil


    cdef double c_levenshtein(
        unsigned char* word_m,
        Py_ssize_t m,
        unsigned char* word_n,
        Py_ssize_t n,
        double[::1] insert_costs,
        double[::1] delete_costs,
        double[:,::1] substitute_costs) nogil
