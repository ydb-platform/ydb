from __future__ import absolute_import
from inspect import CO_VARARGS, CO_VARKEYWORDS, signature
from collections import namedtuple
import re

from .decorators import unwrap


# This provides sufficient introspection for *curry() functions.
#
# We only really need a number of required positional arguments.
# If arguments can be specified by name (not true for many builtin functions),
# then we need to now their names to ignore anything else passed by name.
#
# Stars mean some positional argument which can't be passed by name.
# Functions not mentioned here get one star "spec".
ARGS = {}


ARGS['builtins'] = {
    'bool': '*',
    'complex': 'real,imag',
    'enumerate': 'iterable,start',
    'file': 'file-**',
    'float': 'x',
    'int': 'x-*',
    'long': 'x-*',
    'open': 'file-**',
    'round': 'number-*',
    'setattr': '***',
    'str': 'object-*',
    'unicode': 'string-**',
    '__import__': 'name-****',
    '__buildclass__': '***',
    # Complex functions with different set of arguments
    'iter': '*-*',
    'format': '*-*',
    'type': '*-**',
}
# Add two argument functions
two_arg_funcs = '''cmp coerce delattr divmod filter getattr hasattr isinstance issubclass
                   map pow reduce'''
ARGS['builtins'].update(dict.fromkeys(two_arg_funcs.split(), '**'))


ARGS['functools'] = {'reduce': '**'}


ARGS['itertools'] = {
    'accumulate': 'iterable-*',
    'combinations': 'iterable,r',
    'combinations_with_replacement': 'iterable,r',
    'compress': 'data,selectors',
    'groupby': 'iterable-*',
    'permutations': 'iterable-*',
    'repeat': 'object-*',
}
two_arg_funcs = 'dropwhile filterfalse ifilter ifilterfalse starmap takewhile'
ARGS['itertools'].update(dict.fromkeys(two_arg_funcs.split(), '**'))


ARGS['operator'] = {
    'delslice': '***',
    'getslice': '***',
    'setitem': '***',
    'setslice': '****',
}
two_arg_funcs = """
    _compare_digest add and_ concat contains countOf delitem div eq floordiv ge getitem
    gt iadd iand iconcat idiv ifloordiv ilshift imatmul imod imul indexOf ior ipow irepeat
    irshift is_ is_not isub itruediv ixor le lshift lt matmul mod mul ne or_ pow repeat rshift
    sequenceIncludes sub truediv xor
"""
ARGS['operator'].update(dict.fromkeys(two_arg_funcs.split(), '**'))
ARGS['operator'].update([
    ('__%s__' % op.strip('_'), args) for op, args in ARGS['operator'].items()])
ARGS['_operator'] = ARGS['operator']


# Fixate this
STD_MODULES = set(ARGS)


# Describe some funcy functions, mostly for r?curry()
ARGS['funcy.seqs'] = {
    'map': 'f*', 'lmap': 'f*', 'xmap': 'f*',
    'mapcat': 'f*', 'lmapcat': 'f*',
}
ARGS['funcy.colls'] = {
    'merge_with': 'f*',
}


Spec = namedtuple("Spec", "max_n names req_n req_names varkw")


def get_spec(func, _cache={}):
    func = getattr(func, '__original__', None) or unwrap(func)
    try:
        return _cache[func]
    except (KeyError, TypeError):
        pass

    mod = getattr(func, '__module__', None)
    if mod in STD_MODULES or mod in ARGS and func.__name__ in ARGS[mod]:
        _spec = ARGS[mod].get(func.__name__, '*')
        required, _, optional = _spec.partition('-')
        req_names = re.findall(r'\w+|\*', required)  # a list with dups of *
        max_n = len(req_names) + len(optional)
        req_n = len(req_names)
        spec = Spec(max_n=max_n, names=set(), req_n=req_n, req_names=set(req_names), varkw=False)
        _cache[func] = spec
        return spec
    elif isinstance(func, type):
        # __init__ inherited from builtin classes
        objclass = getattr(func.__init__, '__objclass__', None)
        if objclass and objclass is not func:
            return get_spec(objclass)
        # Introspect constructor and remove self
        spec = get_spec(func.__init__)
        self_set = {func.__init__.__code__.co_varnames[0]}
        return spec._replace(max_n=spec.max_n - 1, names=spec.names - self_set,
                             req_n=spec.req_n - 1, req_names=spec.req_names - self_set)
    elif hasattr(func, '__code__'):
        return _code_to_spec(func)
    else:
        # We use signature last to be fully backwards compatible. Also it's slower
        try:
            sig = signature(func)
            # import ipdb; ipdb.set_trace()
        except (ValueError, TypeError):
            raise ValueError('Unable to introspect %s() arguments'
                % (getattr(func, '__qualname__', None) or getattr(func, '__name__', func)))
        else:
            spec = _cache[func] = _sig_to_spec(sig)
            return spec


def _code_to_spec(func):
    code = func.__code__

    # Weird function like objects
    defaults = getattr(func, '__defaults__', None)
    defaults_n = len(defaults) if isinstance(defaults, tuple) else 0

    kwdefaults = getattr(func, '__kwdefaults__', None)
    if not isinstance(kwdefaults, dict):
        kwdefaults = {}

    # Python 3.7 and earlier does not have this
    posonly_n = getattr(code, 'co_posonlyargcount', 0)

    varnames = code.co_varnames
    pos_n = code.co_argcount
    n = pos_n + code.co_kwonlyargcount
    names = set(varnames[posonly_n:n])
    req_n = n - defaults_n - len(kwdefaults)
    req_names = set(varnames[posonly_n:pos_n - defaults_n] + varnames[pos_n:n]) - set(kwdefaults)
    varkw = bool(code.co_flags & CO_VARKEYWORDS)
    # If there are varargs they could be required
    max_n = n + 1 if code.co_flags & CO_VARARGS else n
    return Spec(max_n=max_n, names=names, req_n=req_n, req_names=req_names, varkw=varkw)


def _sig_to_spec(sig):
    max_n, names, req_n, req_names, varkw = 0, set(), 0, set(), False
    for name, param in sig.parameters.items():
        max_n += 1
        if param.kind == param.VAR_KEYWORD:
            max_n -= 1
            varkw = True
        elif param.kind == param.VAR_POSITIONAL:
            req_n += 1
        elif param.kind == param.POSITIONAL_ONLY:
            if param.default is param.empty:
                req_n += 1
        else:
            names.add(name)
            if param.default is param.empty:
                req_n += 1
                req_names.add(name)
    return Spec(max_n=max_n, names=names, req_n=req_n, req_names=req_names, varkw=varkw)
