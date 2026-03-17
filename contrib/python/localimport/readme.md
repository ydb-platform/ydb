
<img src="https://img.shields.io/badge/License-MIT-yellow.svg" align="right">
<p align="center">
  <b>localimport</b> allows you to import Python modules in an</br>
  isolated environment, preserving the global importer state.
</p>

### Features

- Emulates an isolated environment for Python module imports
- Evaluates `*.pth` files
- Compatible with `pkg_resources` namespaces
- Mocks `pkgutil.extend_path()` to support zipped Python eggs

Check out the [localimport Documentation](http://niklasrosenstein.github.io/python-localimport/).

### Example

Given your Python script, application or plugin comes with a directory that
contains modules for import, you can use localimport to keep the global
importer state clean.

```
app.py
res/modules/
  some_package/
    __init__.py
```

```python
# app.py
with localimport('res/modules') as _importer:
  import some_package
assert 'some_package' not in sys.modules
```

> **Important**: You must keep the reference to the `localimport` object alive,
> especially if you use `from xx import yy` imports.

### Usage

In most cases it would not make sense to use `localimport` as a Python module
when you actually want to import Python modules since the import of the
`localimport` module itself would not be isolated.  
The solution is to use the `localimport` source code directly in your
application code. Usually you will use a minified version.

Pre-minified versions of `localimport` can be found in this [Gist][pre-minified].
Of course you can minify the code by yourself, for example using the [nr][nr]
command-line tools.

    nr py.blob localimport.py -cme localimport > localimport-gzb64-w80.py

Depending on your application, you may want to use a bootstrapper entry point.

```python
# @@@ minified localimport here @@@

with localimport('.') as _importer:
  from my_application_package.__main__ import main
  main()
```

  [pyminifier]: https://pypi.python.org/pypi/pyminifier
  [py-blobbify]: https://pypi.python.org/pypi/py-blobbify
  [pre-minified]: http://bitly.com/localimport-min
  [nr]: https://github.com/NiklasRosenstein/py-nr

### API

#### `localimport(path, parent_dir=None, do_eggs=True, do_pth=True, do_autodisable=True)`

> A context manager that creates an isolated environment for importing
> Python modules. Once the context manager exits, the previous global
> state is restored.
>
> Note that the context can be entered multiple times, but it is not recommended
> generally as the only case where you would want to do that is inside a piece
> of code that gets executed delayed (eg. a function) which imports a module,
> and building the isolated environment and restoring to the previous state has
> some performance impacts.
>
> Also note that the context will only remove packages on exit that have
> actually been imported from the list of paths specified in the *path*
> argument, but not modules from the standard library, for example.
>
> __Parameters__
>
> * *path* &ndash; A list of paths that are added to `sys.path` inside the
>   context manager. Can also be a single string. If one or more relative
>   paths are passed, they are treated relative to the *parent_dir* argument.
> * *parent_dir* &ndash; A path that is concatenated with relative paths passed
>   to the *path* argument. If this argument is omitted or `None`, it will
>   default to the parent directory of the file that called the `localimport()`
>   constructor (using `sys._getframe(1).f_globals['__file__']`).
> * *do_eggs* &ndash; A boolean that indicates whether `.egg` files or
>   directories found in the additional paths are added to `sys.path`.
> * *do_pth* &ndash; A boolean that indicates whether `.pth` files found
>   in the additional paths will be evaluated.
> * *do_autodisable* &ndash; A boolean that indicates that `localimport.autodisable()`
>   should be called automatically be the context manager.
>
> *Changed in 1.7* Added `do_autodisable` parameter.

#### `localimport.autodisable()`

> Uses `localimport.discover()` to automatically detect modules that could be
> imported from the paths in the importer context and calls #disable on all
> of them.
>
> *New in 1.7*

#### `localimport.disable(modules)`


> Disable one or more modules by moving them from the global module cache
> (`sys.modules`) to a dictionary of temporary hidden modules in the isolated
> environment. Once the `localimport()` context manager exits, these modules
> will be restored. Does nothing when a module does not exist.
>
> __Parameters__
>
> * *modules* &ndash; A list of module names or a single module name string.

#### `localimport.discover()`

> A shorthand for `pkgutil.walk_packages(importer.path)`.
>
> *New in 1.7*

---

<p align="center">Copyright &copy; 2018 Niklas Rosenstein</p>
