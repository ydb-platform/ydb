from importlib import resources


def resource_string(file_name: str) -> str:
    """
    Get the value of a file in `collada/resources/{file_name}`
    as a string.

    Parameters
    -----------
    file_name
      The name of the file in `collada/resources/{file_name}`

    Returns
    ----------
    value
      The contents of the file.
    """
    return resources.files("collada").joinpath("resources", file_name).read_text()
