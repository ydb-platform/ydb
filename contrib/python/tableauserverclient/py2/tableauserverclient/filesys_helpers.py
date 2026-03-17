import os
ALLOWED_SPECIAL = (' ', '.', '_', '-')


def to_filename(string_to_sanitize):
    sanitized = (c for c in string_to_sanitize if c.isalnum() or c in ALLOWED_SPECIAL)
    return "".join(sanitized)


def make_download_path(filepath, filename):
    download_path = None

    if filepath is None:
        download_path = filename

    elif os.path.isdir(filepath):
        download_path = os.path.join(filepath, filename)

    else:
        download_path = filepath + os.path.splitext(filename)[1]

    return download_path
