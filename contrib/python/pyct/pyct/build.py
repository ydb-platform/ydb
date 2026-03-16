import os
import shutil
import warnings


def examples(path, root, verbose=False, force=False):
    """
    Copies the notebooks to the supplied path.
    """
    filepath = os.path.abspath(os.path.dirname(root))
    example_dir = os.path.join(filepath, './examples')
    if not os.path.exists(example_dir):
        example_dir = os.path.join(filepath, '../examples')
    if os.path.exists(path):
        if not force:
            print('%s directory already exists, either delete it or set the force flag' % path)
            return
        shutil.rmtree(path)
    ignore = shutil.ignore_patterns('.ipynb_checkpoints', '*.pyc', '*~')
    tree_root = os.path.abspath(example_dir)
    if os.path.isdir(tree_root):
        shutil.copytree(tree_root, path, ignore=ignore, symlinks=True)
    else:
        print('Cannot find %s' % tree_root)


def get_setup_version(root, reponame):
    """
    Helper to get the current version from either git describe or the
    .version file (if available) - allows for param to not be available.

    Normally used in setup.py as follows:

    >>> from pyct.build import get_setup_version
    >>> version = get_setup_version(__file__, reponame)  # noqa

    .. deprecated:: 0.6.0
    """
    warnings.warn(
        "get_setup_version is deprecated and will be removed in a future version.",
        DeprecationWarning,
        stacklevel=2,
    )
    import json
    filepath = os.path.abspath(os.path.dirname(root))
    version_file_path = os.path.join(filepath, reponame, '.version')
    try:
        from param import version
    except ModuleNotFoundError:
        version = None
    if version is not None:
        return version.Version.setup_version(filepath, reponame, archive_commit="$Format:%h$")
    else:
        print("WARNING: param>=1.6.0 unavailable. If you are installing a package, this warning can safely be ignored. If you are creating a package or otherwise operating in a git repository, you should install param>=1.6.0.")
        return json.load(open(version_file_path, 'r'))['version_string']
