import inspect


def add_imported_function_or_module(self, item):
    """Method to add an object to LineProfiler to be profiled.

    This method is used to extend an instance of LineProfiler so it can identify
    whether an object is function/method, class or module and handle it's
    profiling accordingly.

    Args:
        item (Callable | Type | ModuleType):
            object to be profiled.
    """
    if inspect.isfunction(item):
        self.add_function(item)
    elif inspect.isclass(item):
        for k, v in item.__dict__.items():
            if inspect.isfunction(v):
                self.add_function(v)
    elif inspect.ismodule(item):
        self.add_module(item)
    else:
        return
    self.enable_by_count()
