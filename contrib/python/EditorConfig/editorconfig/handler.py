"""EditorConfig file handler

Provides ``EditorConfigHandler`` class for locating and parsing
EditorConfig files relevant to a given filepath.

Licensed under Simplified BSD License (see LICENSE.BSD file).

"""

import os
from collections import OrderedDict

from editorconfig.exceptions import PathError, VersionError
from editorconfig.ini import EditorConfigParser
from editorconfig.version import VERSION
from editorconfig.versiontools import VersionTuple


__all__ = ['EditorConfigHandler']


def get_filenames(path: str, filename: str) -> list[str]:
    """Yield full filepath for filename in each directory in and above path"""
    path_list = []
    while True:
        path_list.append(os.path.join(path, filename))
        newpath = os.path.dirname(path)
        if path == newpath:
            break
        path = newpath
    return path_list


class EditorConfigHandler(object):

    """
    Allows locating and parsing of EditorConfig files for given filename

    In addition to the constructor a single public method is provided,
    ``get_configurations`` which returns the EditorConfig options for
    the ``filepath`` specified to the constructor.

    """

    def __init__(self, filepath: str, conf_filename: str = '.editorconfig',
                 version: VersionTuple = VERSION):
        """Create EditorConfigHandler for matching given filepath"""
        self.filepath: str = filepath
        self.conf_filename: str = conf_filename
        self.version: VersionTuple = version
        self.options: OrderedDict[str, str] = OrderedDict()

    def get_configurations(self) -> OrderedDict[str, str]:

        """
        Find EditorConfig files and return all options matching filepath

        Special exceptions that may be raised by this function include:

        - ``VersionError``: self.version is invalid EditorConfig version
        - ``PathError``: self.filepath is not a valid absolute filepath
        - ``ParsingError``: improperly formatted EditorConfig file found

        """

        self.check_assertions()
        path, filename = os.path.split(self.filepath)
        conf_files = get_filenames(path, self.conf_filename)

        # Attempt to find and parse every EditorConfig file in filetree
        for filename in conf_files:
            parser = EditorConfigParser(self.filepath)
            parser.read(filename)

            # Merge new EditorConfig file's options into current options
            old_options = self.options
            self.options = parser.options
            self.options.update(old_options)

            # Stop parsing if parsed file has a ``root = true`` option
            if parser.root_file:
                break

        self.preprocess_values()
        return self.options

    def check_assertions(self) -> None:

        """Raise error if filepath or version have invalid values"""

        # Raise ``PathError`` if filepath isn't an absolute path
        if not os.path.isabs(self.filepath):
            raise PathError("Input file must be a full path name.")

        # Raise ``VersionError`` if version specified is greater than current
        if self.version is not None and self.version[:3] > VERSION[:3]:
            raise VersionError(
                "Required version is greater than the current version.")

    def preprocess_values(self) -> None:

        """Preprocess option values for consumption by plugins"""

        opts = self.options

        # Lowercase option value for certain options
        for name in ["end_of_line", "indent_style", "indent_size",
                     "insert_final_newline", "trim_trailing_whitespace",
                     "charset"]:
            if name in opts:
                opts[name] = opts[name].lower()

        # Set indent_size to "tab" if indent_size is unspecified and
        # indent_style is set to "tab".
        if (opts.get("indent_style") == "tab" and
                not "indent_size" in opts and self.version >= (0, 10, 0)):
            opts["indent_size"] = "tab"

        # Set tab_width to indent_size if indent_size is specified and
        # tab_width is unspecified
        if ("indent_size" in opts and "tab_width" not in opts and
                opts["indent_size"] != "tab"):
            opts["tab_width"] = opts["indent_size"]

        # Set indent_size to tab_width if indent_size is "tab"
        if ("indent_size" in opts and "tab_width" in opts and
                opts["indent_size"] == "tab"):
            opts["indent_size"] = opts["tab_width"]
