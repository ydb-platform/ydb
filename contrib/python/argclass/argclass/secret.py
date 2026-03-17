"""SecretString class for hiding sensitive values from logs."""

import traceback


class SecretString(str):
    """
    The class mimics the string, with one important difference.
    Attempting to call __str__ of this instance will result in
    the output of placeholer (the default is "******") if the
    call stack contains of logging module. In other words, this
    is an attempt to keep secrets out of the log.

    However, if you try to do an f-string or str() at the moment
    the parameter is passed to the log, the value will be received,
    because there is nothing about logging in the stack.

    The repr will always give placeholder, so it is better to always
    add ``!r`` for any f-string, for example `f'{value!r}'`.

    Examples:

    >>> import logging
    >>> from argclass import SecretString
    >>> logging.basicConfig(level=logging.INFO)
    >>> s = SecretString("my-secret-password")
    >>> logging.info(s)          # __str__ will be called from logging
    INFO:root:'******'
    >>> logging.info(f"s=%s", s) # __str__ will be called from logging too
    INFO:root:s='******'
    >>> logging.info(f"{s!r}")   # repr is safe
    INFO:root:'******'
    >>> logging.info(f"{s}")     # the password will be compromised
    INFO:root:my-secret-password

    """

    PLACEHOLDER = "******"
    MODULES_SKIPLIST = ("logging", "log.py")

    def __str__(self) -> str:
        for frame in traceback.extract_stack(None):
            for skip in self.MODULES_SKIPLIST:
                if skip in frame.filename:
                    return self.PLACEHOLDER
        return super().__str__()

    def __repr__(self) -> str:
        return repr(self.PLACEHOLDER)
