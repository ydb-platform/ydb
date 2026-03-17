import contextlib
import os.path
import posixpath
import sys


@contextlib.contextmanager
def open_file(path, mode, encoding="utf-8"):
    """
    Behaves like open(), but with some special cases for stdout and stderr.

    :param path: string of the path to open
    :param mode: string of the mode to open the file in
    :param encoding: encoding to use when opening the file (text mode only)
    :return: a context manager that yields the file object
    """
    output_file = None
    if path in ("/dev/stdout", "-"):
        output_file = sys.stdout
    elif path == "/dev/stderr":
        output_file = sys.stderr

    if output_file:
        if "b" in mode:
            output_file = output_file.buffer
        yield output_file
    else:
        if "b" in mode:
            encoding = None

        with open(path, mode, encoding=encoding) as f:
            yield f


def to_unix_path(path):
    """
    Tries to ensure tha the path is a normalized unix path.
    This seems to be the solution cobertura used....
    https://github.com/cobertura/cobertura/blob/642a46eb17e14f51272c6962e64e56e0960918af/cobertura/src/main/java/net/sourceforge/cobertura/instrument/ClassPattern.java#L84

    I know of at least one case where this will fail (\\) is allowed in unix paths.
    But I am taking the bet that this is not common. We deal with source code.

    :param path: string of the path to convert
    :return: the unix version of that path
    """
    return posixpath.normpath(os.path.normcase(path).replace("\\", "/"))


def to_unix_paths(paths):
    return [to_unix_path(path) for path in paths]


def to_unescaped_filename(filename: str) -> str:
    """Try to unescape the given filename.

    Some filenames given by git might be escaped with C-style escape sequences
    and surrounded by double quotes.
    """
    if not (filename.startswith('"') and filename.endswith('"')):
        return filename

    # Remove surrounding quotes
    unquoted = filename[1:-1]

    # Handle C-style escape sequences
    result = []
    i = 0
    while i < len(unquoted):
        if unquoted[i] == "\\" and i + 1 < len(unquoted):
            # Handle common C escape sequences
            next_char = unquoted[i + 1]
            result.append(
                {
                    "\\": "\\",
                    '"': '"',
                    "a": "a",
                    "n": "\n",
                    "t": "\t",
                    "r": "\r",
                    "b": "\b",
                    "f": "\f",
                }.get(next_char, next_char)
            )
            i += 2
        else:
            result.append(unquoted[i])
            i += 1

    return "".join(result)
