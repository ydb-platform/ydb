from pathlib import PureWindowsPath, Path
from urllib.request import url2pathname
from urllib.parse import urlparse, unquote


def path_from_fileurl(urlstr, **kwargs):
    """
    Take a file:// url and return a Path.

    Adapted from:
        https://github.com/AcademySoftwareFoundation/OpenTimelineIO/blob/4c17494dee2e515aedc8623741556fae3e4afe72/src/py-opentimelineio/opentimelineio/url_utils.py#L43-L72
    """
    # explicitly unquote first in case drive colon is url encoded
    unquoted = unquote(urlstr)

    # Parse provided URL
    parsed_result = urlparse(unquoted)

    # Convert the parsed URL to a path
    filepath = Path(url2pathname(parsed_result.path), **kwargs)

    # If the network location is a window drive, reassemble the path
    if PureWindowsPath(parsed_result.netloc).drive:
        filepath = Path(parsed_result.netloc + parsed_result.path, **kwargs)

    # Otherwise check if the specified index is a windows drive, then offset the path
    elif len(filepath.parts) > 1 and PureWindowsPath(filepath.parts[1]).drive:
        # Remove leading "/" if/when `request.url2pathname` yields "/S:/path/file.ext"
        filepath = Path(*filepath.parts[1:], **kwargs)

    return filepath
