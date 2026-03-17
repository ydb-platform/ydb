import ast
import os
import sys

from .util_static import (modname_to_modpath, modpath_to_modname,
                          package_modpaths)


class ProfmodExtractor:
    """Map prof_mod to imports in an abstract syntax tree.

    Takes the paths and dotted paths in prod_mod and finds their respective imports in an
    abstract syntax tree.
    """

    def __init__(self, tree, script_file, prof_mod):
        """Initializes the AST tree profiler instance with the AST, script file path and prof_mod

        Args:
            tree (_ast.Module):
                abstract syntax tree to fetch imports from.

            script_file (str):
                path to script being profiled.

            prof_mod (List[str]):
                list of imports to profile in script.
                passing the path to script will profile the whole script.
                the objects can be specified using its dotted path or full path (if applicable).
        """
        self._tree = tree
        self._script_file = script_file
        self._prof_mod = prof_mod

    @staticmethod
    def _is_path(text):
        """Check whether a string is a path.

        Checks if a string contains a slash or ends with .py indicating it is a path.

        Args:
            text (str):
                string to check whether it is a path or not

        Returns:
            ret (bool):
                bool indicating whether the string is a path or not
        """
        ret = ('/' in text.replace('\\', '/')) or text.endswith('.py')
        return ret

    @classmethod
    def _get_modnames_to_profile_from_prof_mod(cls, script_file, prof_mod):
        """Grab the valid paths and all dotted paths in prof_mod and their subpackages
        and submodules, in the form of dotted paths.

        First all items in prof_mod are converted to a valid path. if unable to convert,
        check if the item is an invalid path and skip it, else assume it is an installed package.
        The valid paths are then converted to dotted paths.
        The converted dotted paths along with the items assumed to be installed packages
        are added a list of modnames_to_profile.
        Then all subpackages and submodules under each valid path is fetched, converted to
        dotted path and also added to the list.
        if script_file is in prof_mod it is skipped to avoid name collision with othe imports,
        it will be processed elsewhere in the autoprofile pipeline.

        Args:
            script_file (str):
                path to script being profiled.

            prof_mod (List[str]):
                list of imports to profile in script.
                passing the path to script will profile the whole script.
                the objects can be specified using its dotted path or full path (if applicable).

        Returns:
            modnames_to_profile (List[str]):
                list of dotted paths to profile.
        """
        script_directory = os.path.realpath(os.path.dirname(script_file))
        """add script folder to modname_to_modpath sys_path to allow it to resolve modpaths"""
        new_sys_path = [script_directory] + sys.path
        script_file_realpath = os.path.realpath(script_file)

        modnames_to_profile = []
        for mod in prof_mod:
            if script_file_realpath == os.path.realpath(mod):
                """
                skip script_file as it will add the script's name without its extension which
                could have the same name as another import or function leading to unwanted profiling
                """
                continue
            """
            convert the item in prof_mod into a valid path.
            if it fails, the item may point to an installed module rather than local script
            so we check if the item is path and whether that path exists, else skip the item.
            """
            modpath = modname_to_modpath(mod, sys_path=new_sys_path)
            if modpath is None:
                """if cannot convert to modpath, check if already path and if invalid"""
                if not os.path.exists(mod):
                    if cls._is_path(mod):
                        """modpath does not exist, so skip"""
                        continue
                    modnames_to_profile.append(mod)
                    continue
                """assume item is and installed package. modpath_to_modname will have no effect"""
                modpath = mod

            """convert path to dotted path and add it to list to be profiled"""
            try:
                modname = modpath_to_modname(modpath)
            except ValueError:
                continue
            if modname not in modnames_to_profile:
                modnames_to_profile.append(modname)

            """
            recursively fetch all subpackages and submodules, convert them to dotted paths
            and add them to list to be profiled
            """
            for submod_path in package_modpaths(modpath):
                submod_name = modpath_to_modname(submod_path)
                if submod_name not in modnames_to_profile:
                    modnames_to_profile.append(submod_name)

        return modnames_to_profile

    @staticmethod
    def _ast_get_imports_from_tree(tree):
        """Get all imports in an abstract syntax tree.

        Args:
            tree (_ast.Module):
                abstract syntax tree to fetch imports from.

        Returns:
            module_dict_list (List[Dict[str,Union[str,int]]]):
                list of dicts of all imports in the tree, containing:
                    name (str):
                        the real name of the import. e.g. foo from "import foo as bar"
                    alias (str):
                        the alias of an import if applicable. e.g. bar from "import foo as bar"
                    tree_index (int):
                        the index of the import as found in the tree
        """
        module_dict_list = []
        modname_list = []
        for idx, node in enumerate(tree.body):
            if isinstance(node, ast.Import):
                for name in node.names:
                    modname = name.name
                    if modname not in modname_list:
                        alias = name.asname
                        module_dict = {
                            'name': modname,
                            'alias': alias,
                            'tree_index': idx,
                        }
                        module_dict_list.append(module_dict)
                        modname_list.append(modname)
            elif isinstance(node, ast.ImportFrom):
                for name in node.names:
                    modname = node.module + '.' + name.name
                    if modname not in modname_list:
                        alias = name.asname or name.name
                        module_dict = {
                            'name': modname,
                            'alias': alias,
                            'tree_index': idx,
                        }
                        module_dict_list.append(module_dict)
                        modname_list.append(modname)
        return module_dict_list

    @staticmethod
    def _find_modnames_in_tree_imports(modnames_to_profile, module_dict_list):
        """Map modnames to imports from an abstract sytax tree.

        Find imports in modue_dict_list, created from an abstract syntax tree, that match
        dotted paths in modnames_to_profile.
        When a submodule is imported, both the submodule and the parent module are checked
        whether they are in modnames_to_profile. As the user can ask to profile
        "foo" when only "from foo import bar" is imported, so both foo and bar are checked.
        The real import name of an import is used to map to the dotted paths.
        The import's alias is stored in the output dict.

        Args:
            modnames_to_profile (List[str]):
                list of dotted paths to profile.

            module_dict_list (List[Dict[str,Union[str,int]]]):
                list of dicts of all imports in the tree.

        Returns:
            modnames_found_in_tree (Dict[int,str]):
                dict of imports found
                    key (int):
                        index of import in AST
                    value (str):
                        alias (or name if no alias used) of import
        """
        modnames_found_in_tree = {}
        modname_added_list = []
        for i, module_dict in enumerate(module_dict_list):
            modname = module_dict['name']
            if modname in modname_added_list:
                continue
            """check if either the parent module or submodule are in modnames_to_profile"""
            if modname not in modnames_to_profile and modname.rsplit('.', 1)[0] not in modnames_to_profile:
                continue
            name = module_dict['alias'] or modname
            modname_added_list.append(modname)
            modnames_found_in_tree[module_dict['tree_index']] = name
        return modnames_found_in_tree

    def run(self):
        """Map prof_mod to imports in an abstract syntax tree.

        Takes the paths and dotted paths in prod_mod and finds their respective imports in an
        abstract syntax tree, returning their alias and the index they appear in the AST.

        Returns:
            (Dict[int,str]): tree_imports_to_profile_dict
                dict of imports to profile
                    key (int):
                        index of import in AST
                    value (str):
                        alias (or name if no alias used) of import
        """
        modnames_to_profile = self._get_modnames_to_profile_from_prof_mod(self._script_file, self._prof_mod)

        module_dict_list = self._ast_get_imports_from_tree(self._tree)

        tree_imports_to_profile_dict = self._find_modnames_in_tree_imports(modnames_to_profile, module_dict_list)
        return tree_imports_to_profile_dict
