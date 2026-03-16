"""Python bindings for tree-sitter."""

from ctypes import cdll, c_void_p
from ctypes.util import find_library
import setuptools
from distutils.ccompiler import new_compiler
from distutils.unixccompiler import UnixCCompiler
from os import path
from platform import system
from tempfile import TemporaryDirectory
from tree_sitter.binding import _language_field_id_for_name, _language_query
from tree_sitter.binding import Node, Parser, Tree, TreeCursor  # noqa: F401


class Language:
    """A tree-sitter language"""

    @staticmethod
    def build_library(output_path, repo_paths):
        """
        Build a dynamic library at the given path, based on the parser
        repositories at the given paths.

        Returns `True` if the dynamic library was compiled and `False` if
        the library already existed and was modified more recently than
        any of the source files.
        """
        output_mtime = path.getmtime(output_path) if path.exists(output_path) else 0

        if not repo_paths:
            raise ValueError("Must provide at least one language folder")

        cpp = False
        source_paths = []
        for repo_path in repo_paths:
            src_path = path.join(repo_path, "src")
            source_paths.append(path.join(src_path, "parser.c"))
            if path.exists(path.join(src_path, "scanner.cc")):
                cpp = True
                source_paths.append(path.join(src_path, "scanner.cc"))
            elif path.exists(path.join(src_path, "scanner.c")):
                source_paths.append(path.join(src_path, "scanner.c"))
        source_mtimes = [path.getmtime(__file__)] + [
            path.getmtime(path_) for path_ in source_paths
        ]

        compiler = new_compiler()
        if isinstance(compiler, UnixCCompiler):
            compiler.compiler_cxx[0] = "c++"

        if max(source_mtimes) <= output_mtime:
            return False

        with TemporaryDirectory(suffix="tree_sitter_language") as out_dir:
            object_paths = []
            for source_path in source_paths:
                if system() == "Windows":
                    flags = None
                else:
                    flags = ["-fPIC"]
                    if source_path.endswith(".c"):
                        flags.append("-std=c99")
                object_paths.append(
                    compiler.compile(
                        [source_path],
                        output_dir=out_dir,
                        include_dirs=[path.dirname(source_path)],
                        extra_preargs=flags,
                    )[0]
                )
            compiler.link_shared_object(
                object_paths,
                output_path,
                target_lang="c++" if cpp else "c",
            )
        return True

    def __init__(self, library_path, name):
        """
        Load the language with the given name from the dynamic library
        at the given path.
        """
        self.name = name     
        path = find_library("grammars")
        if not path:
            raise ImportError("Cannot find tree_sitter library")
        self.lib = cdll.LoadLibrary(path)
        language_function = getattr(self.lib, "tree_sitter_%s" % name)
        language_function.restype = c_void_p
        self.language_id = language_function()

    def field_id_for_name(self, name):
        """Return the field id for a field name."""
        return _language_field_id_for_name(self.language_id, name)

    def query(self, source):
        """Create a Query with the given source code."""
        return _language_query(self.language_id, source)
