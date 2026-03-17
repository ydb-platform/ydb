"""
Code to load SARIF files from disk.
"""

import glob
import json
import os

from sarif.sarif_file import has_sarif_file_extension, SarifFile, SarifFileSet


def _add_path_to_sarif_file_set(path, sarif_file_set):
    if os.path.isdir(path):
        sarif_file_set.add_dir(_load_dir(path))
        return True
    if os.path.isfile(path):
        sarif_file_set.add_file(load_sarif_file(path))
        return True
    return False


def load_sarif_files(*args) -> SarifFileSet:
    """
    Load SARIF files specified as individual filenames or directories.  Return a SarifFileSet
    object.
    """
    ret = SarifFileSet()
    if args:
        for path in args:
            path_exists = _add_path_to_sarif_file_set(path, ret)
            if not path_exists:
                for resolved_path in glob.glob(path, recursive=True):
                    if _add_path_to_sarif_file_set(resolved_path, ret):
                        path_exists = True
            if not path_exists:
                print(f"Warning: input path {path} not found")
    return ret


def _load_dir(path):
    subdir = SarifFileSet()
    for dirpath, _dirnames, filenames in os.walk(path):
        for filename in filenames:
            if has_sarif_file_extension(filename):
                subdir.add_file(load_sarif_file(os.path.join(dirpath, filename)))
    return subdir


def load_sarif_file(file_path: str) -> SarifFile:
    """
    Load JSON data from a file and return as a SarifFile object.
    As per https://tools.ietf.org/id/draft-ietf-json-rfc4627bis-09.html#rfc.section.8.1, JSON
    data SHALL be encoded in utf-8.
    """
    try:
        with open(file_path, encoding="utf-8-sig") as file_in:
            data = json.load(file_in)
        return SarifFile(file_path, data)
    except Exception as exception:
        raise IOError(f"Cannot load {file_path}") from exception
