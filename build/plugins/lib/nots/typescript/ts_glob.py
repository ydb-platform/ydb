import fnmatch
import os.path


class TsGlobConfig:
    def __init__(self, root_dir, out_dir=None, include=None):
        # type: (TsGlobConfig, str, str, list[str], list[str]) -> None

        self.root_dir = root_dir  # Required
        self.out_dir = out_dir

        self.include = include or ["**/*"]


def __path_to_match_rule(path):
    # type: (str) -> str

    # already a rule

    # convert "**/*" to "*" (python compatible with fnmatch)
    if path.endswith('**/*'):
        return path[:-3]  # /**/* -> /*

    if path.endswith("*") or ('*' in path or '?' in path):
        return path

    # special cases
    if path == ".":
        return "*"

    # filename
    root, ext = os.path.splitext(path)
    if ext:
        return path

    # dirname ?
    return os.path.join(path, '*')


def __filter_files(files, path_or_rule):
    # type: (set[str], str) -> set[str]

    rule = __path_to_match_rule(path_or_rule)

    result = set()
    for path in files:
        py_rule = __path_to_match_rule(rule)
        if path == rule or fnmatch.fnmatch(path, py_rule):
            result.add(path)

    return result


def ts_glob(glob_config, all_files):
    # type: (TsGlobConfig, list[str]) -> list[str]

    result = set(all_files)

    # only in `root_dir`
    result &= __filter_files(result, glob_config.root_dir)

    # only listed by `include` option
    include_only = set()
    for include_path in glob_config.include:
        include_only |= __filter_files(result, include_path)

    result &= include_only  # keep only intersection (common in both sets)

    skip_files = set()

    # exclude out_dir
    if glob_config.out_dir:
        skip_files |= __filter_files(result, glob_config.out_dir)

    result -= skip_files  # keep only differences (the elements in `result` that not exist in `skip_files`)

    return sorted(result)
