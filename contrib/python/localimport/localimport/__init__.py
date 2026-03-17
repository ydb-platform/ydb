
__author__ = 'Niklas Rosenstein <rosensteinniklas@gmail.com>'
__version__ = '1.7.6'

import copy
import glob
import os
import pkgutil
import sys
import traceback
import typing as t
import zipfile

if t.TYPE_CHECKING:
  from sys import _MetaPathFinder


def is_local(filename: str, pathlist: t.List[str]) -> bool:
  ''' Returns True if *filename* is a subpath of any of the paths in *pathlist*. '''

  filename = os.path.abspath(filename)
  for path_name in pathlist:
    path_name = os.path.abspath(path_name)
    if is_subpath(filename, path_name):
      return True
  return False


def is_subpath(path: str, parent: str) -> bool:
  ''' Returns True if *path* points to the same or a subpath of *parent*. '''

  try:
    relpath = os.path.relpath(path, parent)
  except ValueError:
    return False  # happens on Windows if drive letters don't match
  return relpath == os.curdir or not relpath.startswith(os.pardir)


def eval_pth(
  filename: str,
  sitedir: str,
  dest: t.Optional[t.List[str]] = None,
  imports: t.Optional[t.List[t.Tuple[str, int, str]]] = None,
) -> t.List[str]:
  ''' Evaluates a `.pth` file (including support for `import` statements), and appends the result to the list
  *dest*. If *dest* is #None, it will fall back to `sys.path`.

  If *imports* is specified, it must be a list. `import` statements will not executed but instead appended to
  that list in tuples of (*filename*, *line*, *stmt*).
  '''

  if dest is None:
    dest = sys.path

  if not os.path.isfile(filename):
    return []

  with open(filename, 'r') as fp:
    for index, line in enumerate(fp):
      if line.startswith('import'):
        if imports is None:
          exec_pth_import(filename, index+1, line)
        else:
          imports.append((filename, index+1, line))
      else:
        index = line.find('#')
        if index > 0: line = line[:index]
        line = line.strip()
        if not os.path.isabs(line):
          line = os.path.join(os.path.dirname(filename), line)
        line = os.path.normpath(line)
        if line and line not in dest:
          dest.insert(0, line)

  return dest


def exec_pth_import(filename: str, lineno: int, line: str) -> None:
  line = '\n' * (lineno - 1) + line.strip()
  try:
    exec(compile(line, filename, 'exec'))
  except BaseException:
    traceback.print_exc()


def extend_path(pth: t.List[str], name: str) -> t.List[str]:
  ''' Better implementation of #pkgutil.extend_path()  which adds support for zipped Python eggs. The original
  #pkgutil.extend_path() gets mocked by this function inside the #localimport context.
  '''

  def zip_isfile(z, name):
    name.rstrip('/')
    return name in z.namelist()

  pname = os.path.join(*name.split('.'))
  zname = '/'.join(name.split('.'))
  init_py = '__init__' + os.extsep + 'py'
  init_pyc = '__init__' + os.extsep + 'pyc'
  init_pyo = '__init__' + os.extsep + 'pyo'

  mod_path = list(pth)
  for path in sys.path:
    if zipfile.is_zipfile(path):
      try:
        egg = zipfile.ZipFile(path, 'r')
        addpath = (
          zip_isfile(egg, zname + '/__init__.py') or
          zip_isfile(egg, zname + '/__init__.pyc') or
          zip_isfile(egg, zname + '/__init__.pyo'))
        fpath = os.path.join(path, path, zname)
        if addpath and fpath not in mod_path:
          mod_path.append(fpath)
      except (zipfile.BadZipfile, zipfile.LargeZipFile):
        pass  # xxx: Show a warning at least?
    else:
      path = os.path.join(path, pname)
      if os.path.isdir(path) and path not in mod_path:
        addpath = (
          os.path.isfile(os.path.join(path, init_py)) or
          os.path.isfile(os.path.join(path, init_pyc)) or
          os.path.isfile(os.path.join(path, init_pyo)))
        if addpath and path not in mod_path:
          mod_path.append(path)

  return [os.path.normpath(x) for x in mod_path]


class localimport:

  def __init__(
    self,
    path: t.Union[t.List[str], str],
    parent_dir: t.Optional[str] = None,
    do_eggs: bool = True,
    do_pth: bool = True,
    do_autodisable: bool = True,
  ) -> None:
    if not parent_dir:
      frame = sys._getframe(1).f_globals
      if '__file__' in frame:
        parent_dir = os.path.dirname(os.path.abspath(frame['__file__']))

    # Convert relative paths to absolute paths with parent_dir and
    # evaluate .egg files in the specified directories.
    self.path = []
    if isinstance(path, str):
      path = [path]
    for path_name in path:
      if not os.path.isabs(path_name):
        if not parent_dir:
          raise ValueError('relative path but no parent_dir')
        path_name = os.path.join(parent_dir, path_name)
      path_name = os.path.normpath(path_name)
      self.path.append(path_name)
      if do_eggs:
        self.path.extend(glob.glob(os.path.join(path_name, '*.egg')))

    self.meta_path: t.List[_MetaPathFinder] = []
    self.modules: t.Dict[str, t.Any] = {}
    self.do_pth = do_pth
    self.in_context = False
    self.do_autodisable = do_autodisable
    self.pth_imports: t.List[t.Tuple[str, int, str]] = []

    if self.do_pth:
      seen = set()
      for path_name in self.path:
        for fn in glob.glob(os.path.join(path_name, '*.pth')):
          if fn in seen: continue
          seen.add(fn)
          eval_pth(fn, path_name, dest=self.path, imports=self.pth_imports)

  def __enter__(self) -> 'localimport':
    # pkg_resources comes with setuptools.
    try:
      import pkg_resources
      nsdict = copy.deepcopy(pkg_resources._namespace_packages)  # type: ignore
      declare_namespace = pkg_resources.declare_namespace
      pkg_resources.declare_namespace = self._declare_namespace  # type: ignore
    except ImportError:
      nsdict = None
      declare_namespace = None

    # Save the global importer state.
    self.state = {
      'nsdict': nsdict,
      'declare_namespace': declare_namespace,
      'nspaths': {},
      'path': sys.path[:],
      'meta_path': sys.meta_path[:],
      'disables': {},
      'pkgutil.extend_path': pkgutil.extend_path,
    }

    # Update the systems meta path and apply function mocks.
    sys.path[:] = self.path
    sys.meta_path[:] = self.meta_path + sys.meta_path
    pkgutil.extend_path = extend_path  # type: ignore

    # If this function is called not the first time, we need to
    # restore the modules that have been imported with it and
    # temporarily disable the ones that would be shadowed.
    for key, mod in list(self.modules.items()):
      try: self.state['disables'][key] = sys.modules.pop(key)
      except KeyError: pass
      sys.modules[key] = mod

    # Evaluate imports from the .pth files, if any.
    for fn, lineno, stmt in self.pth_imports:
      exec_pth_import(fn, lineno, stmt)

    # Add the original path to sys.path.
    sys.path += self.state['path']

    # Update the __path__ of all namespace modules.
    for key, mod in list(sys.modules.items()):
      if mod is None:
        # Relative imports could have lead to None-entries in
        # sys.modules. Get rid of them so they can be re-evaluated.
        prefix = key.rpartition('.')[0]
        if hasattr(sys.modules.get(prefix), '__path__'):
          del sys.modules[key]
      elif hasattr(mod, '__path__'):
        self.state['nspaths'][key] = copy.copy(mod.__path__)
        mod.__path__ = pkgutil.extend_path(mod.__path__, mod.__name__)

    self.in_context = True
    if self.do_autodisable:
      self.autodisable()
    return self

  def __exit__(self, *__) -> None:
    if not self.in_context:
      raise RuntimeError('context not entered')

    # Figure the difference of the original sys.path and the
    # current path. The list of paths will be used to determine
    # what modules are local and what not.
    local_paths = []
    for path in sys.path:
      if path not in self.state['path']:
        local_paths.append(path)
    for path in self.path:
      if path not in local_paths:
        local_paths.append(path)

    # Move all meta path objects to self.meta_path that have not
    # been there before and have not been in the list before.
    for meta in sys.meta_path:
      if meta is not self and meta not in self.state['meta_path']:
        if meta not in self.meta_path:
          self.meta_path.append(meta)

    # Move all modules that shadow modules of the original system
    # state or modules that are from any of the localimport context
    # paths away.
    modules = sys.modules.copy()
    for key, mod in modules.items():
      force_pop = False
      filename = getattr(mod, '__file__', None)
      if not filename and key not in sys.builtin_module_names:
        parent = key.rsplit('.', 1)[0]
        if parent in modules:
          filename = getattr(modules[parent], '__file__', None)
        else:
          force_pop = True
      if force_pop or (filename and is_local(filename, local_paths)):
        self.modules[key] = sys.modules.pop(key)

    # Restore the disabled modules.
    sys.modules.update(self.state['disables'])
    for key, mod in self.state['disables'].items():
      try: parent_name = key.split('.')[-2]
      except IndexError: parent_name = None
      if parent_name and parent_name in sys.modules:
        parent_module = sys.modules[parent_name]
        setattr(parent_module, key.split('.')[-1], mod)

    # Restore the original __path__ value of namespace packages.
    for key, path_list in self.state['nspaths'].items():
      try: sys.modules[key].__path__ = path_list
      except KeyError: pass

    # Restore the original state of the global importer.
    sys.path[:] = self.state['path']
    sys.meta_path[:] = self.state['meta_path']
    pkgutil.extend_path = self.state['pkgutil.extend_path']
    try:
      import pkg_resources
      pkg_resources.declare_namespace = self.state['declare_namespace']
      pkg_resources._namespace_packages.clear()  # type: ignore
      pkg_resources._namespace_packages.update(self.state['nsdict'])  # type: ignore
    except ImportError: pass

    self.in_context = False
    del self.state

  def _declare_namespace(self, package_name: str) -> None:
    '''
    Mock for #pkg_resources.declare_namespace() which calls
    #pkgutil.extend_path() afterwards as the original implementation doesn't
    seem to properly find all available namespace paths.
    '''

    self.state['declare_namespace'](package_name)
    mod = sys.modules[package_name]
    mod.__path__ = pkgutil.extend_path(mod.__path__, package_name)  # type: ignore

  def discover(self) -> t.Iterable[pkgutil.ModuleInfo]:
    return pkgutil.iter_modules(self.path)

  def disable(self, module: t.Union[t.List[str], str]) -> None:
    if not isinstance(module, str):
      for module_name in module:
        self.disable(module_name)
      return

    sub_prefix = module + '.'
    modules = {}
    for key, mod in sys.modules.items():
      if key == module or key.startswith(sub_prefix):
        try: parent_name = '.'.join(key.split('.')[:-1])
        except IndexError: parent_name = None

        # Delete the child module reference from the parent module.
        modules[key] = mod
        if parent_name and parent_name in sys.modules:
          parent = sys.modules[parent_name]
          try:
            delattr(parent, key.split('.')[-1])
          except AttributeError:
            pass

    # Pop all the modules we found from sys.modules
    for key, mod in modules.items():
      del sys.modules[key]
      self.state['disables'][key] = mod

  def autodisable(self) -> None:
    for loader, name, ispkg in self.discover():
      self.disable(name)
