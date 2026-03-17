import io

from library.python import resource


def prefetch_from_memory(path):
    """
    If `path` exists in one of RESOURCE macros, return a file-like wrapper around it;
    otherwise, return the path as it is.
    """

    if not isinstance(path, str):
        return path

    file_path = path
    if isinstance(file_path, str):
        file_path = file_path.encode("ascii", errors="strict")

    data = resource.resfs_read(file_path)
    return io.BytesIO(data) if data is not None else path
