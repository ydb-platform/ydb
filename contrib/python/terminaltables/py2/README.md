## terminaltables

# What is it

Easily draw tables in terminal/console applications from a list of lists of strings. Supports multi-line rows.

- Python 2.6, 2.7, PyPy, PyPy3, 3.3, 3.4, and 3.5+ supported on Linux and OS X.
- Python 2.7, 3.3, 3.4, and 3.5+ supported on Windows (both 32 and 64 bit versions of Python).

📖 Full documentation: https://robpol86.github.io/terminaltables

Quickstart
==========

Install:

```bash
pip install terminaltables
```

Usage:

```python
from terminaltables import AsciiTable

table_data = [
    ['Heading1', 'Heading2'],
    ['row1 column1', 'row1 column2'],
    ['row2 column1', 'row2 column2'],
    ['row3 column1', 'row3 column2']
]
table = AsciiTable(table_data)
print
table.table
```

```bash
+--------------+--------------+
| Heading1     | Heading2     |
+--------------+--------------+
| row1 column1 | row1 column2 |
| row2 column1 | row2 column2 |
| row3 column1 | row3 column2 |
+--------------+--------------+
```

Example Implementations
=======================
![Example Scripts Screenshot](https://github.com/matthewdeanmartin/terminaltables/blob/master/docs/examples.png?raw=true)

Source code for examples:

- [example1.py](https://github.com/matthewdeanmartin/terminaltables/blob/master/example1.py)
- [example2.py](https://github.com/matthewdeanmartin/terminaltables/blob/master/example2.py)
- [example3.py](https://github.com/matthewdeanmartin/terminaltables/blob/master/example3.py)

[Change Log](https://github.com/matthewdeanmartin/terminaltables/blob/master/CHANGELOG.md)