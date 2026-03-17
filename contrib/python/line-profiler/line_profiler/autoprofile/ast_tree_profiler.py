import ast
import os

from .ast_profle_transformer import (AstProfileTransformer,
                                     ast_create_profile_node)
from .profmod_extractor import ProfmodExtractor

__docstubs__ = """
from .ast_profle_transformer import AstProfileTransformer
from .profmod_extractor import ProfmodExtractor
"""


class AstTreeProfiler:
    """Create an abstract syntax tree of a script and add profiling to it.

    Reads a script file and generates an abstract syntax tree, then adds nodes
    and/or decorators to the AST that adds the specified functions/methods,
    classes & modules in prof_mod to the profiler to be profiled.
    """

    def __init__(self,
                 script_file,
                 prof_mod,
                 profile_imports,
                 ast_transformer_class_handler=AstProfileTransformer,
                 profmod_extractor_class_handler=ProfmodExtractor):
        """Initializes the AST tree profiler instance with the script file path

        Args:
            script_file (str):
                path to script being profiled.

            prof_mod (List[str]):
                list of imports to profile in script.
                passing the path to script will profile the whole script.
                the objects can be specified using its dotted path or full path (if applicable).

            profile_imports (bool):
                if True, when auto-profiling whole script, profile all imports aswell.

            ast_transformer_class_handler (Type):
                the AstProfileTransformer class that handles profiling the whole script.

            profmod_extractor_class_handler (Type):
                the ProfmodExtractor class that handles mapping prof_mod to objects in the script.
        """
        self._script_file = script_file
        self._prof_mod = prof_mod
        self._profile_imports = profile_imports
        self._ast_transformer_class_handler = ast_transformer_class_handler
        self._profmod_extractor_class_handler = profmod_extractor_class_handler

    @staticmethod
    def _check_profile_full_script(script_file, prof_mod):
        """Check whether whole script should be profiled.

        Checks whether path to script has been passed to prof_mod indicating that
        the whole script should be profiled

        Args:
            script_file (str):
                path to script being profiled.

            prof_mod (List[str]):
                list of imports to profile in script.
                passing the path to script will profile the whole script.
                the objects can be specified using its dotted path or full path (if applicable).

        Returns:
            (bool): profile_full_script
                if True, profile whole script.
        """
        script_file_realpath = os.path.realpath(script_file)
        profile_full_script = script_file_realpath in map(os.path.realpath, prof_mod)
        return profile_full_script

    @staticmethod
    def _get_script_ast_tree(script_file):
        """Generate an abstract syntax from a script file.

        Args:
            script_file (str):
                path to script being profiled.

        Returns:
            tree (_ast.Module):
                abstract syntax tree of the script.
        """
        with open(script_file, 'r') as f:
            script_text = f.read()
        tree = ast.parse(script_text, filename=script_file)
        return tree

    def _profile_ast_tree(self,
                          tree,
                          tree_imports_to_profile_dict,
                          profile_full_script=False,
                          profile_imports=False):
        """Add profiling to an abstract syntax tree.

        Adds nodes to the AST that adds the specified objects to the profiler.
        If profile_full_script is True, all functions/methods, classes & modules in the script
        have a node added to the AST to add them to the profiler.
        If profile_imports is True as well as profile_full_script, all imports are have a node
        added to the AST to add them to the profiler.

        Args:
            tree (_ast.Module):
                abstract syntax tree to be profiled.

            tree_imports_to_profile_dict (Dict[int,str]):
                dict of imports to profile
                    key (int):
                        index of import in AST
                    value (str):
                        alias (or name if no alias used) of import

            profile_full_script (bool):
                if True, profile whole script.

            profile_imports (bool):
                if True, and profile_full_script is True, profile all imports aswell.

        Returns:
            (_ast.Module): tree
                abstract syntax tree with profiling.
        """
        profiled_imports = []
        argsort_tree_indexes = sorted(list(tree_imports_to_profile_dict), reverse=True)
        for tree_index in argsort_tree_indexes:
            name = tree_imports_to_profile_dict[tree_index]
            expr = ast_create_profile_node(name)
            tree.body.insert(tree_index + 1, expr)
            profiled_imports.append(name)
        if profile_full_script:
            tree = self._ast_transformer_class_handler(profile_imports=profile_imports,
                                                       profiled_imports=profiled_imports).visit(tree)
        ast.fix_missing_locations(tree)
        return tree

    def profile(self):
        """Create an abstract syntax tree of a script and add profiling to it.

        Reads a script file and generates an abstract syntax tree.
        Then matches imports in the script's AST with the names in prof_mod.
        The matched imports are added to the profiler for profiling.
        If path to script is found in prof_mod, all functions/methods, classes & modules are
        added to the profiler.
        If profile_imports is True as well as path to script in prof_mod, all the imports
        in the script are added to the profiler.

        Returns:
            (_ast.Module): tree
                abstract syntax tree with profiling.
        """
        profile_full_script = self._check_profile_full_script(self._script_file, self._prof_mod)

        tree = self._get_script_ast_tree(self._script_file)

        tree_imports_to_profile_dict = self._profmod_extractor_class_handler(
            tree, self._script_file, self._prof_mod).run()
        tree_profiled = self._profile_ast_tree(tree,
                                               tree_imports_to_profile_dict,
                                               profile_full_script=profile_full_script,
                                               profile_imports=self._profile_imports)
        return tree_profiled
