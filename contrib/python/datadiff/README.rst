DataDiff
========

DataDiff is a library to provide human-readable diffs of python data structures.
It can handle sequence types (lists, tuples, etc), sets, and dictionaries.

Dictionaries and sequences will be diffed recursively, when applicable.

It has special-case handling for multi-line strings, showing them as a typical unified diff.

Drop-in replacements for some nose assertions are available.  If the assertion fails,
a nice data diff is shown, letting you easily pinpoint the root difference.

``datadiff`` works on Python 2.6 through Python 3.

DataDiff project homepage: http://sourceforge.net/projects/datadiff/

Example
-------

Here's an example::

    >>> from datadiff import diff
    >>> a = dict(foo=1, bar=2, baz=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    >>> b = dict(foo=1, bar=4, baz=[0, 1, 2, 3, 4, 5, 6, 7, 8])
    >>> print diff(a, b)
    --- a
    +++ b
    {
    -'bar': 2,
    +'bar': 4,
     'baz': [
     @@ -5,11 +5,8 @@
      6,
      7,
      8,
     -9,
     -10,
     -11,
     ],
     'foo': 1,
    }
    >>>
    >>> from datadiff.tools import assert_equal
    >>> assert_equal([1, 2, 3], [1, 2, 5])
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "datadiff/tools.py", line 29, in assert_equal
        raise AssertionError(msg)
    AssertionError:
    --- a
    +++ b
    [
    @@ -0,2 +0,2 @@
     1,
     2,
    -3,
    +5,
    ]

License
-------

Copyright Dave Brondsema

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
