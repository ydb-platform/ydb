## This file was originally made to fascilitate support for both python2 and python3.


def to_wire(text):
    if text is None:
        return None
    if isinstance(text, str):
        text = bytes(text, "utf-8")
    text = text.replace(b"\n", b"\r\n")
    text = text.replace(b"\r\r\n", b"\r\n")
    return text


def to_local(text):
    if text is None:
        return None
    if not isinstance(text, str):
        text = text.decode("utf-8")
    text = text.replace("\r\n", "\n")
    return text


to_str = to_local


def to_normal_str(text):
    """
    Make sure we return a normal string
    """
    if text is None:
        return text
    if not isinstance(text, str):
        text = text.decode("utf-8")
    text = text.replace("\r\n", "\n")
    return text


def to_unicode(text):
    if text and isinstance(text, bytes):
        return text.decode("utf-8")
    return text
