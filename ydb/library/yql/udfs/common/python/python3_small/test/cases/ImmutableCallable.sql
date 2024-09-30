$script = @@#py
class ImmutableCallable:
    def __init__(self, func):
        # XXX: Explicitly set field value via __dict__, since __setattr__ is
        # forbidden.
        self.__dict__['func'] = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __setattr__(self, *args):
        raise AttributeError("Immutable callable")

@ImmutableCallable
def echo(arg):
    return arg
@@;

$echo = Python3::echo(Callable<(String)->String>, $script);

SELECT $echo("YQL");
