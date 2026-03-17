def hide(*exceptions):
    """
        Example:

    .. code-block:: python

       __tracebackhide__ = traceback.hide(BaseError)
    """

    def tracebackhide(excinfo):
        return excinfo.errisinstance(exceptions)

    return tracebackhide
