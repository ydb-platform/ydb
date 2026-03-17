
=====
Edlib
=====

Lightweight, super fast library for sequence alignment using edit (Levenshtein) distance.

Popular use cases: aligning DNA sequences, calculating word/text similarity.


.. code:: python

    edlib.align("elephant", "telephone")
    # {'editDistance': 3, 'alphabetLength': 8, 'locations': [(None, 8)], 'cigar': None}

    # Works with unicode characters (or any other iterable of hashable objects)!
    edlib.align("ты милая", "ты гений")
    # {'editDistance': 5, 'alphabetLength': 12, 'locations': [(None, 7)], 'cigar': None}

    edlib.align("AACG", "TCAACCTG", mode = "HW", task = "path")
    # {'editDistance': 1, 'alphabetLength': 4, 'locations': [(2, 4), (2, 5)], 'cigar': '3=1I'}

    query = "elephant"; target = "telephone"
    # NOTE: `task` has to be "path" in order to get nice alignment.
    result = edlib.align(query, target, task = "path")
    nice = edlib.getNiceAlignment(result, query, target)
    print("\n".join(nice.values()))
    # -elephant
    # -|||||.|.
    # telephone


Edlib is actually a C/C++ library, and this package is it's wrapper for Python.
Python Edlib has mostly the same API as C/C++ Edlib, so feel free to check out `C/C++ Edlib docs <http://github.com/Martinsos/edlib>`_ for more code examples, details on API and how Edlib works.

--------
Features
--------

* Calculates **edit distance**.
* It can find **optimal alignment path** (instructions how to transform first sequence into the second sequence).
* It can find just the **start and/or end locations of alignment path** - can be useful when speed is more important than having exact alignment path.
* Supports **multiple alignment methods**: global(**NW**), prefix(**SHW**) and infix(**HW**), each of them useful for different scenarios.
* You can **extend character equality definition**, enabling you to e.g. have wildcard characters, to have case insensitive alignment or to work with degenerate nucleotides.
* It can easily handle small or **very large** sequences, even when finding alignment path.
* **Super fast** thanks to Myers's bit-vector algorithm.

**NOTE**: **Alphabet length has to be <= 256** (meaning that query and target together must have <= 256 unique values).

------------
Installation
------------
::

    pip install edlib

---
API
---

Edlib has two functions, ``align()`` and ``getNiceAlignment()``:

align()
-------

.. code:: python

    align(query, target, [mode], [task], [k], [additionalEqualities])

Aligns ``query`` against ``target`` with edit distance.

``query`` and ``target`` can be strings, bytes, or any iterables of hashable objects, as long as all together they don't have more than 256 unique values.


Output of ``help(edlib.align)``:

.. code::

    cython_function_or_method in module edlib
    
    align(query, target, mode='NW', task='distance', k=-1, additionalEqualities=None)
        Align query with target using edit distance.
        @param {str or bytes or iterable of hashable objects} query, combined with target must have no more
               than 256 unique values
        @param {str or bytes or iterable of hashable objects} target, combined with query must have no more
               than 256 unique values
        @param {string} mode  Optional. Alignment method do be used. Possible values are:
                - 'NW' for global (default)
                - 'HW' for infix
                - 'SHW' for prefix.
        @param {string} task  Optional. Tells edlib what to calculate. The less there is to calculate,
                the faster it is. Possible value are (from fastest to slowest):
                - 'distance' - find edit distance and end locations in target. Default.
                - 'locations' - find edit distance, end locations and start locations.
                - 'path' - find edit distance, start and end locations and alignment path.
        @param {int} k  Optional. Max edit distance to search for - the lower this value,
                the faster is calculation. Set to -1 (default) to have no limit on edit distance.
        @param {list} additionalEqualities  Optional.
                List of pairs of characters or hashable objects, where each pair defines two values as equal.
                This way you can extend edlib's definition of equality (which is that each character is equal only
                to itself).
                This can be useful e.g. when you want edlib to be case insensitive, or if you want certain
                characters to act as a wildcards.
                Set to None (default) if you do not want to extend edlib's default equality definition.
        @return Dictionary with following fields:
                {int} editDistance  Integer, -1 if it is larger than k.
                {int} alphabetLength Integer, length of unique characters in 'query' and 'target'
                {[(int, int)]} locations  List of locations, in format [(start, end)].
                {string} cigar  Cigar is a standard format for alignment path.
                    Here we are using extended cigar format, which uses following symbols:
                    Match: '=', Insertion to target: 'I', Deletion from target: 'D', Mismatch: 'X'.
                    e.g. cigar of "5=1X1=1I" means "5 matches, 1 mismatch, 1 match, 1 insertion (to target)".
    

getNiceAlignment()
------------------

.. code:: python

    getNiceAlignment(alignResult, query, target)

Represents alignment from ``align()`` in a visually attractive format.


Output of ``help(edlib.getNiceAlignment)``:

.. code::

    cython_function_or_method in module edlib
    
    getNiceAlignment(alignResult, query, target, gapSymbol='-')
        Output alignments from align() in NICE format
        @param {dictionary} alignResult, output of the method align()
            NOTE: The method align() requires the argument task="path"
        @param {string} query, the exact query used for alignResult
        @param {string} target, the exact target used for alignResult
        @param {string} gapSymbol, default "-"
            String used to represent gaps in the alignment between query and target
        @return Alignment in NICE format, which is human-readable visual representation of how the query and target align to each other.
            e.g., for "telephone" and "elephant", it would look like:
               telephone
                |||||.|.
               -elephant
            It is represented as dictionary with following fields:
              - {string} query_aligned
              - {string} matched_aligned ('|' for match, '.' for mismatch, ' ' for insertion/deletion)
              - {string} target_aligned
            Normally you will want to print these three in order above joined with newline character.
    


-----
Usage
-----


.. code:: python

    import edlib

    edlib.align("ACTG", "CACTRT", mode="HW", task="path")
    # {'editDistance': 1, 'alphabetLength': 5, 'locations': [(1, 3), (1, 4)], 'cigar': '3=1I'}

    # You can provide additional equalities.
    edlib.align("ACTG", "CACTRT", mode="HW", task="path", additionalEqualities=[("R", "A"), ("R", "G")])
    # {'editDistance': 0, 'alphabetLength': 5, 'locations': [(1, 4)], 'cigar': '4='}

   

---------
Benchmark
---------

I run a simple benchmark on 7 Feb 2017 (using timeit, on Python3) to get a feeling of how Edlib compares to other Python libraries: `editdistance <https://pypi.python.org/pypi/editdistance>`_ and `python-Levenshtein <https://pypi.python.org/pypi/python-Levenshtein>`_.

As input data I used pairs of DNA sequences of different lengths, where each pair has about 90% similarity.

::

   #1: query length: 30, target length: 30
   edlib.align(query, target): 1.88µs
   editdistance.eval(query, target): 1.26µs
   Levenshtein.distance(query, target): 0.43µs

   #2: query length: 100, target length: 100
   edlib.align(query, target): 3.64µs
   editdistance.eval(query, target): 3.86µs
   Levenshtein.distance(query, target): 14.1µs

   #3: query length: 1000, target length: 1000
   edlib.align(query, target): 0.047ms
   editdistance.eval(query, target): 5.4ms
   Levenshtein.distance(query, target): 1.9ms

   #4: query length: 10000, target length: 10000
   edlib.align(query, target): 0.0021s
   editdistance.eval(query, target): 0.56s
   Levenshtein.distance(query, target): 0.2s

   #5: query length: 50000, target length: 50000
   edlib.align(query, target): 0.031s
   editdistance.eval(query, target): 13.8s
   Levenshtein.distance(query, target): 5.0s

----
More
----

Check out `C/C++ Edlib docs <http://github.com/Martinsos/edlib>`_ for more information about Edlib!

-----------
Development
-----------

Check out `Edlib python package on Github <https://github.com/Martinsos/edlib/tree/master/bindings/python>`_.

