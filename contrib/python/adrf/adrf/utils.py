import inspect


# NOTE This function was taken from the python library and modified
# to allow an exclusion list and avoid recursion errors.
def getmembers(object, predicate, exclude_names=[]):
    results = []
    processed = set()
    names = [x for x in dir(object) if x not in exclude_names]
    if inspect.isclass(object):
        mro = inspect.getmro(object)
        # add any DynamicClassAttributes to the list of names if object is a class;
        # this may result in duplicate entries if, for example, a virtual
        # attribute with the same name as a DynamicClassAttribute exists
        try:
            for base in object.__bases__:
                for k, v in base.__dict__.items():
                    if (
                        isinstance(v, inspect.types.DynamicClassAttribute)
                        and k not in exclude_names
                    ):
                        names.append(k)
        except AttributeError:
            pass
    else:
        mro = ()
    for key in names:
        # First try to get the value via getattr.  Some descriptors don't
        # like calling their __get__ (see bug #1785), so fall back to
        # looking in the __dict__.
        try:
            value = getattr(object, key)
            # handle the duplicate key
            if key in processed:
                raise AttributeError
        except AttributeError:
            for base in mro:
                if key in base.__dict__:
                    value = base.__dict__[key]
                    break
            else:
                # could be a (currently) missing slot member, or a buggy
                # __dir__; discard and move on
                continue
        if not predicate or predicate(value):
            results.append((key, value))
        processed.add(key)
    results.sort(key=lambda pair: pair[0])
    return results
