# pasta: **P**ython **AST** **A**ugmentation

*This is still a work-in-progress; there is much more to do. Existing
functionality may not be perfect.*

## Mission
Enable python source code refactoring through AST modifications.

Sample use cases:

* Facilitate moving or renaming python modules by rewriting import statements.
* Refactor code to enforce a certain style, such as reordering function
  definitions.
* Safely migrate code from one API to another.

## Design Goals

* **Symmetry**: Given any input source, it should hold that
  `pasta.dump(pasta.parse(src)) == src`.
* **Mutability**: Any changes made in the AST are reflected in the code
  generated from it.
* **Standardization**: The syntax tree parsed by pasta will not introduce new
  nodes or structure that the user must learn.

## Python Version Support

Supports python `2.7` and up to `3.8`.

## Dependencies

`pasta` depends on [`six`](https://pypi.org/project/six/).

## Basic Usage

```python
import pasta
tree = pasta.parse(source_code)

# ... Augment contents of tree ...

source_code = pasta.dump(tree)
```

## Built-in Augmentations

Pasta includes some common augmentations out-of-the-box. These can be used as
building blocks for more complex refactoring actions.

*There will be more of these basic augmentations added over time. Stay tuned!*

### Rename an imported name

Rewrites references to an imported name, module or package. For some more
examples, see [`pasta/augment/rename_test.py`](pasta/augment/rename_test.py).

```python
# Rewrite references from one module to another
rename.rename_external(tree, 'pkg.subpkg.module', 'pkg.other_module')

# Rewrite references from one package to another
rename.rename_external(tree, 'pkg.subpkg', 'pkg.other_pkg')

# Rewrite references to an imported name in another module
rename.rename_external(tree, 'pkg.module.Query', 'pkg.module.ExecuteQuery')
```

## Known issues and limitations

* Changing the indentation level of a block of code is not supported. This is
  not an issue for renames, but would cause problems for refactors like
  extracting a method.

* `pasta` works under the assumption that the python version that the code is
  written for and the version used to run pasta are the same. This is because
  pasta relies on [`ast.parse`](https://docs.python.org/2/library/ast.html#ast.parse)

* Some python features are not fully supported, including `global`.

## Developing

This project uses
[`setuptools`](https://setuptools.readthedocs.io/en/latest/setuptools.html) to
facilitate testing and packaging.

```python
# Run all tests
python setup.py test

# Run a single test suite
python setup.py test -s pasta.base.annotate_test.suite
```

## Disclaimer

This is not an official Google product.
