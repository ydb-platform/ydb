"""
exceptions.py
----------------

Wrap exceptions.
"""


class ExceptionWrapper:
    """
    Create a dummy object which will raise an exception when attributes
    are accessed (i.e. when used as a module) or when called (i.e.
    when used like a function)

    For soft dependencies we want to survive failing to import but
    we would like to raise an appropriate error when the functionality is
    actually requested so the user gets an easily debuggable message.
    """

    def __init__(self, exception: BaseException):
        # store the exception type and the args rather than the whole thing
        # this prevents the locals from the time of the exception
        # from being stored as well.
        self.exception = (type(exception), exception.args)

    def __getattribute__(self, *args, **kwargs):
        # will raise when this object is accessed like an object
        # if it's asking for our class type return None
        # this allows isinstance() checks to not re-raise
        if args[0] == "__class__":
            return None.__class__

        # re-create our original exception from the type and arguments
        exc_type, exc_args = super().__getattribute__("exception")
        raise exc_type(*exc_args)

    def __call__(self, *args, **kwargs):
        # behave the same when this object is called like a function
        # as when someone tries to access an attribute like a module
        self.__getattribute__("exception")
