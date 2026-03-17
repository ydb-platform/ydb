from copy import deepcopy
from urllib.parse import unquote_plus


def fix_filename(params):
    if "filename*" not in params:
        return params

    params = deepcopy(params)
    filename = params["filename*"]
    prefix = "UTF-8''"
    if filename.startswith(prefix):
        filename = filename[len(prefix) :]

    params["filename"] = unquote_plus(filename)
    del params["filename*"]
    return params
