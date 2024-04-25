import copy
import os

from .ts_errors import TsError, TsValidationError
from .ts_glob import ts_glob, TsGlobConfig
from ..package_manager.base import utils

DEFAULT_TS_CONFIG_FILE = "tsconfig.json"


class RootFields:
    extends = 'extends'

    exclude = 'exclude'
    files = 'files'
    include = 'include'

    compilerOptions = 'compilerOptions'

    PATH_LIST_FIELDS = {
        exclude,
        files,
        include,
    }


class CompilerOptionsFields:
    baseUrl = 'baseUrl'
    declaration = 'declaration'
    declarationDir = 'declarationDir'
    outDir = 'outDir'
    rootDir = 'rootDir'

    PATH_FIELDS = {
        baseUrl,
        outDir,
        rootDir,
    }


class TsConfig(object):
    @classmethod
    def load(cls, path):
        """
        :param path: tsconfig.json path
        :type path: str
        :rtype: TsConfig
        """
        tsconfig = cls(path)
        tsconfig.read()

        return tsconfig

    def __init__(self, path):
        import rapidjson

        self.rj = rapidjson

        if not os.path.isabs(path):
            raise TypeError("Absolute path required, given: {}".format(path))

        self.path = path
        self.data = {}

    def read(self):
        try:
            with open(self.path) as f:
                self.data = self.rj.load(f, parse_mode=(self.rj.PM_COMMENTS | self.rj.PM_TRAILING_COMMAS))

        except Exception as e:
            raise TsError("Failed to read tsconfig {}: {}".format(self.path, e))

    def merge(self, rel_path, base_tsconfig):
        # type: (TsConfig, str, TsConfig) -> None
        """
        :param rel_path: relative path to the configuration file we are merging in.
        It is required to set the relative paths correctly.

        :param base_tsconfig: base TsConfig we are merging with our TsConfig instance
        """
        if not base_tsconfig.data:
            return

        # 'data' from the file in 'extends'
        base_data = copy.deepcopy(base_tsconfig.data)

        def relative_path(p):
            return os.path.normpath(os.path.join(rel_path, p))

        for root_field, root_value in base_data.items():
            # extends
            if root_field == RootFields.extends:
                # replace itself to its own `extends` (for multi level extends)
                self.data[RootFields.extends] = relative_path(root_value)

            # exclude, files, include
            elif root_field in RootFields.PATH_LIST_FIELDS:
                if root_field not in self.data:
                    self.data[root_field] = [relative_path(p) for p in root_value]

            # compilerOptions
            elif root_field == RootFields.compilerOptions:
                for option, option_value in root_value.items():
                    is_path_field = option in CompilerOptionsFields.PATH_FIELDS

                    if not self.has_compiler_option(option):
                        new_value = relative_path(option_value) if is_path_field else option_value
                        self.set_compiler_option(option, new_value)

            # other fields (just copy if it has not existed)
            elif root_field not in self.data:
                self.data[root_field] = root_value
        pass

    def extend_one(self, dep_paths, ext_value):
        if not ext_value:
            return []

        if ext_value.startswith("."):
            base_config_path = ext_value

        else:
            dep_name = utils.extract_package_name_from_path(ext_value)
            # the rest part is the ext config path
            file_path_start = len(dep_name) + 1
            file_path = ext_value[file_path_start:]
            dep_path = dep_paths.get(dep_name)
            if dep_path is None:
                raise Exception(
                    "referenceing from {}, data: {}\n: Dependency '{}' not found in dep_paths: {}".format(
                        self.path, str(self.data), dep_name, dep_paths
                    )
                )
            base_config_path = os.path.join(dep_path, file_path)

        rel_path = os.path.dirname(base_config_path)
        tsconfig_curdir_path = os.path.join(os.path.dirname(self.path), base_config_path)
        if os.path.isdir(tsconfig_curdir_path):
            base_config_path = os.path.join(base_config_path, DEFAULT_TS_CONFIG_FILE)

        # processing the base file recursively
        base_config = TsConfig.load(os.path.join(os.path.dirname(self.path), base_config_path))
        paths = [base_config_path] + base_config.inline_extend(dep_paths)

        self.merge(rel_path, base_config)
        return paths

    def inline_extend(self, dep_paths):
        """
        Merges the tsconfig parameters from configuration file referred by "extends" if any.
        Relative paths are adjusted, current parameter values are prioritized higer than
        those coming from extension file (according to TSC mergin rules).
        Returns list of file paths for config files merged into the current configuration
        :param dep_paths: dict of dependency names to their paths
        :type dep_paths: dict
        :rtype: list of str
        """
        extends = self.data.get(RootFields.extends)

        if type(extends) == list:
            paths = [self.extend_one(dep_paths, ext_value) for ext_value in extends]
            flatten_paths = [item for row in paths for item in row]
        else:
            flatten_paths = self.extend_one(dep_paths, extends)

        if extends:
            del self.data[RootFields.extends]

        return flatten_paths

    def get_or_create_compiler_options(self):
        """
        Returns ref to the "compilerOptions" dict.
        :rtype: dict
        """
        if RootFields.compilerOptions not in self.data:
            self.data[RootFields.compilerOptions] = {}

        return self.data[RootFields.compilerOptions]

    def compiler_option(self, name, default=None):
        """
        :param name: option key
        :type name: str
        :param default: default value
        :type default: mixed
        :rtype: mixed
        """
        return self.get_or_create_compiler_options().get(name, default)

    def has_compiler_option(self, name):
        # type: (str) -> bool
        compiler_options = self.data.get(RootFields.compilerOptions, {})

        return name in compiler_options

    def set_compiler_option(self, name, value):
        # type: (str, Any) -> None
        compiler_options = self.get_or_create_compiler_options()
        compiler_options[name] = value

    def validate(self, use_outdir=False):
        # type: (bool) -> void
        """
        Checks whether the config is compatible with current toolchain.
        """
        opts = self.get_or_create_compiler_options()
        errors = []
        declaration = opts.get(CompilerOptionsFields.declaration)
        declaration_dir = opts.get(CompilerOptionsFields.declarationDir)
        out_dir = opts.get(CompilerOptionsFields.outDir)
        root_dir = opts.get(CompilerOptionsFields.rootDir)
        config_dir = os.path.dirname(self.path)

        def is_mod_subdir(p):
            return not os.path.isabs(p) and os.path.normpath(os.path.join(config_dir, p)).startswith(config_dir)

        if root_dir is None:
            errors.append("'rootDir' option is required")

        if use_outdir:
            if out_dir is None:
                errors.append("'outDir' option is required")
            elif out_dir in [".", "", "./"]:
                errors.append("'outDir' value '{}' is not supported, use directory name like 'build'".format(out_dir))
            elif not is_mod_subdir(out_dir):
                errors.append("'outDir' should be a subdirectory of the module")
        else:
            if out_dir:
                errors.append("'outDir' should be removed - it is not in use")
            # Checking only when outDir shouldn't be used, as when we allow outDir,
            # it routes all the results including declarations.
            if declaration is True and declaration_dir is None:
                errors.append("'declarationDir' option is required when 'declaration' is set")

        if opts.get("outFile") is not None:
            errors.append("'outFile' option is not supported")

        if opts.get("preserveSymlinks"):
            errors.append("'preserveSymlinks' option is not supported due to pnpm limitations")

        if self.data.get("references") is not None:
            errors.append("composite builds are not supported, use peerdirs in ya.make instead of 'references' option")

        if len(errors):
            raise TsValidationError(self.path, errors)

    def write(self, path=None, indent=None):
        """
        :param path: tsconfig path, defaults to original path
        :type path: str
        """
        if path is None:
            path = self.path

        with open(path, "w") as f:
            self.rj.dump(self.data, f, indent=indent)

    def filter_files(self, all_files):
        # type: (list[str]) -> list[str]
        """
        Filters all the files by the rules from this tsconig.json. The result will be used as input entries in `ya make`.

        Known limits:

        - `exclude` not implemented, because `tsc` still uses "excluded" files as declaration files (for typing and referencing)
        """

        ts_glob_config = TsGlobConfig(
            root_dir=self.compiler_option(CompilerOptionsFields.rootDir),
            out_dir=self.compiler_option(CompilerOptionsFields.outDir),
            include=self.data.get(RootFields.include),
            files=self.data.get(RootFields.files),
        )

        return ts_glob(ts_glob_config, all_files)

    def get_out_dirs(self):
        # type: () -> list[str]
        output_dirs = [self.compiler_option("outDir"), self.compiler_option("declarationDir")]

        return [d for d in output_dirs if d is not None]
