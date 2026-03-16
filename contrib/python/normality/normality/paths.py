import os
from typing import Optional
from banal import decode_path
from normality.stringify import stringify
from normality.cleaning import squash_spaces, category_replace
from normality.constants import UNICODE_CATEGORIES, WS
from normality.transliteration import ascii_text

MAX_LENGTH = 254


def _safe_name(file_name: Optional[str], sep: str) -> Optional[str]:
    """Convert the file name to ASCII and normalize the string."""
    file_name = stringify(file_name)
    if file_name is None:
        return None
    file_name = ascii_text(file_name)
    file_name = category_replace(file_name, UNICODE_CATEGORIES)
    file_name = squash_spaces(file_name)
    file_name = file_name.replace(WS, sep).strip(sep)
    if len(file_name) == 0:
        return None
    return file_name


def safe_filename(
    file_name: Optional[str],
    sep: str = "_",
    default: Optional[str] = None,
    extension: Optional[str] = None,
) -> Optional[str]:
    """Create a secure filename for plain file system storage."""
    if file_name is None:
        return decode_path(default)

    file_name = decode_path(file_name)
    if file_name is None:
        return None
    file_name = os.path.basename(file_name)
    file_name, _extension = os.path.splitext(file_name)
    file_name = _safe_name(file_name, sep=sep)
    if file_name is None:
        return decode_path(default)
    file_name = file_name[:MAX_LENGTH]
    extension = _safe_name(extension or _extension, sep=sep)
    if extension is not None:
        file_name = ".".join((file_name, extension))
        file_name = file_name[:MAX_LENGTH]
    return file_name
