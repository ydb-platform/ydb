py-tree-sitter
==================

[![Build Status](https://github.com/tree-sitter/py-tree-sitter/actions/workflows/ci.yml/badge.svg)](https://github.com/tree-sitter/py-tree-sitter/actions/workflows/ci.yml)
[![Build status](https://ci.appveyor.com/api/projects/status/mde790v0v9gux85w/branch/master?svg=true)](https://ci.appveyor.com/project/maxbrunsfeld/py-tree-sitter/branch/master)

This module provides Python bindings to the [tree-sitter](https://github.com/tree-sitter/tree-sitter) parsing library.

## Installation

This package currently only works with Python 3. There are no library dependencies, but you do need to have a C compiler installed.

```sh
pip3 install tree_sitter
```

## Usage

#### Setup

First you'll need a Tree-sitter language implementation for each language that you want to parse. You can clone some of the [existing language repos](https://github.com/tree-sitter) or [create your own](http://tree-sitter.github.io/tree-sitter/creating-parsers):

```sh
git clone https://github.com/tree-sitter/tree-sitter-go
git clone https://github.com/tree-sitter/tree-sitter-javascript
git clone https://github.com/tree-sitter/tree-sitter-python
```

Use the `Language.build_library` method to compile these into a library that's usable from Python. This function will return immediately if the library has already been compiled since the last time its source code was modified:

```python
from tree_sitter import Language, Parser

Language.build_library(
  # Store the library in the `build` directory
  'build/my-languages.so',

  # Include one or more languages
  [
    'vendor/tree-sitter-go',
    'vendor/tree-sitter-javascript',
    'vendor/tree-sitter-python'
  ]
)
```

Load the languages into your app as `Language` objects:

```python
GO_LANGUAGE = Language('build/my-languages.so', 'go')
JS_LANGUAGE = Language('build/my-languages.so', 'javascript')
PY_LANGUAGE = Language('build/my-languages.so', 'python')
```

#### Basic Parsing

Create a `Parser` and configure it to use one of the languages:

```python
parser = Parser()
parser.set_language(PY_LANGUAGE)
```

Parse some source code:

```python
tree = parser.parse(bytes("""
def foo():
    if bar:
        baz()
""", "utf8"))
```

If you have your source code in some data structure other than a bytes object,
you can pass a "read" callable to the parse function.

The read callable can use either the byte offset or point tuple to read from
buffer and return source code as bytes object. An empty bytes object or None
terminates parsing for that line. The bytes must encode the source as UTF-8.

For example, to use the byte offset:

```python
src = bytes("""
def foo():
    if bar:
        baz()
""", "utf8")

def read_callable(byte_offset, point):
    return src[byte_offset:byte_offset+1]

tree = parser.parse(read_callable)
```

And to use the point:

```python
src_lines = ["def foo():\n", "    if bar:\n", "        baz()"]

def read_callable(byte_offset, point):
    row, column = point
    if row >= len(src_lines) or column >= len(src_lines[row]):
        return None
    return src_lines[row][column:].encode('utf8')

tree = parser.parse(read_callable)
```

Inspect the resulting `Tree`:

```python
root_node = tree.root_node
assert root_node.type == 'module'
assert root_node.start_point == (1, 0)
assert root_node.end_point == (3, 13)

function_node = root_node.children[0]
assert function_node.type == 'function_definition'
assert function_node.child_by_field_name('name').type == 'identifier'

function_name_node = function_node.children[1]
assert function_name_node.type == 'identifier'
assert function_name_node.start_point == (1, 4)
assert function_name_node.end_point == (1, 7)

assert root_node.sexp() == "(module "
    "(function_definition "
        "name: (identifier) "
        "parameters: (parameters) "
        "body: (block "
            "(if_statement "
                "condition: (identifier) "
                "consequence: (block "
                    "(expression_statement (call "
                        "function: (identifier) "
                        "arguments: (argument_list))))))))"
```

#### Walking Syntax Trees

If you need to traverse a large number of nodes efficiently, you can use
a `TreeCursor`:

```python
cursor = tree.walk()

assert cursor.node.type == 'module'

assert cursor.goto_first_child()
assert cursor.node.type == 'function_definition'

assert cursor.goto_first_child()
assert cursor.node.type == 'def'

# Returns `False` because the `def` node has no children
assert not cursor.goto_first_child()

assert cursor.goto_next_sibling()
assert cursor.node.type == 'identifier'

assert cursor.goto_next_sibling()
assert cursor.node.type == 'parameters'

assert cursor.goto_parent()
assert cursor.node.type == 'function_definition'
```

#### Editing

When a source file is edited, you can edit the syntax tree to keep it in sync with the source:

```python
tree.edit(
    start_byte=5,
    old_end_byte=5,
    new_end_byte=5 + 2,
    start_point=(0, 5),
    old_end_point=(0, 5),
    new_end_point=(0, 5 + 2),
)
```

Then, when you're ready to incorporate the changes into a new syntax tree,
you can call `Parser.parse` again, but pass in the old tree:

```python
new_tree = parser.parse(new_source, tree)
```

This will run much faster than if you were parsing from scratch.

The `Tree.get_changed_ranges` method can be called on the *old* tree to return
the list of ranges whose syntactic structure has been changed:

```python
for changed_range in tree.get_changed_ranges(new_tree):
    print('Changed range:')
    print(f'  Start point {changed_range.start_point}')
    print(f'  Start byte {changed_range.start_byte}')
    print(f'  End point {changed_range.end_point}')
    print(f'  End byte {changed_range.end_byte}')
```

#### Pattern-matching

You can search for patterns in a syntax tree using a *tree query*:

```python
query = PY_LANGUAGE.query("""
(function_definition
  name: (identifier) @function.def)

(call
  function: (identifier) @function.call)
""")

captures = query.captures(tree.root_node)
assert len(captures) == 2
assert captures[0][0] == function_name_node
assert captures[0][1] == "function.def"
```

The `Query.captures()` method takes optional `start_point`, `end_point`,
`start_byte` and `end_byte` keyword arguments which can be used to restrict the
query's range. Only one of the `..._byte` or `..._point` pairs need to be given
to restrict the range. If all are omitted, the entire range of the passed node
is used.
