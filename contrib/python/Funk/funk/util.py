from . import pycompat


def arguments_str(args, kwargs, separator=", "):
    args_strs = [str(arg) for arg in args]
    
    sorted_kwargs = sorted(pycompat.iteritems(kwargs), key=lambda x: x[0])
    kwargs_strs = ['%s=%s' % (key, value) for key, value in sorted_kwargs]
        
    all_args_strs = args_strs + kwargs_strs
    return separator.join(all_args_strs)

def function_call_str(name, args, kwargs):
    return "%s(%s)" % (name, arguments_str(args, kwargs))

def method_call_str(object_name, method_name, args, kwargs):
    return "%s.%s" % (object_name, function_call_str(method_name, args, kwargs))

def function_call_str_multiple_lines(name, args, kwargs):
    return "%s(%s)" % (name, arguments_str(args, kwargs, ",\n" + " " * (len(name) + 1)))
    
def map_values(func, dict_):
    return dict(
        (key, func(value))
        for key, value in pycompat.iteritems(dict_)
    )
