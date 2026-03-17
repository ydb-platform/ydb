import inspect

annotation_value_types = (int, float, bool, str)


def is_classmethod(func):
    return getattr(func, '__self__', None) is not None


def is_instance_method(parent_class, func_name, func):
    try:
        func_from_dict = parent_class.__dict__[func_name]
    except KeyError:
        for base in inspect.getmro(parent_class):
            if func_name in base.__dict__:
                func_from_dict = base.__dict__[func_name]
                break
        else:
            return True

    return not is_classmethod(func) and not isinstance(func_from_dict, staticmethod)
