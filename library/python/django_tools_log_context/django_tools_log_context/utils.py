import six

if six.PY3:
    from importlib import import_module


def dynamic_import_py2(abs_module_path, class_name):
    module_object = __import__(abs_module_path, fromlist=[class_name])
    return getattr(module_object, class_name)


def dynamic_import_py3(abs_module_path, class_name):
    module_object = import_module(abs_module_path)
    return getattr(module_object, class_name)


def dynamic_import(abs_module_path, class_name):
    try:
        if six.PY2:
            return dynamic_import_py2(abs_module_path, class_name)
        elif six.PY3:
            return dynamic_import_py3(abs_module_path, class_name)
    except (ImportError, AttributeError):
        return None
