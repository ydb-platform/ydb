from funk.error import FunkyError
from funk.util import function_call_str
from funk.util import function_call_str_multiple_lines
from funk.matchers import to_matcher
from .pycompat import iteritems
from .util import map_values

class InfiniteCallCount(object):
    def none_remaining(self):
        return False
        
    def decrement(self):
        pass
        
    def is_satisfied(self):
        return True

class IntegerCallCount(object):
    def __init__(self, count):
        self._count = count
    
    def none_remaining(self):
        return self._count <= 0
        
    def decrement(self):
        self._count -= 1
        
    def is_satisfied(self):
        return self.none_remaining()

class Call(object):
    _arguments_set = False
    
    def __init__(self, name, call_count=InfiniteCallCount()):
        self._name = name
        self._call_count = call_count
        self._action = lambda: None
        self._sequences = []
    
    def has_name(self, name):
        return self._name == name
    
    def accepts(self, args, kwargs, mismatch_description):
        if self._call_count.none_remaining():
            mismatch_description.append("%s [expectation has already been satisfied]" % str(self))
            return False
        if not self._arguments_set:
            return True
            
        def describe_arg(matcher, result):
            if result.is_match:
                explanation = "matched"
            else:
                explanation = result.explanation
            
            return "%s [%s]" % (matcher.describe(), explanation)
        
        def describe_kwargs(kwarg_matches):
            return dict(
                (key, describe_arg(self._allowed_kwargs[key], result))
                for key, result in kwarg_matches
            )
        
        def describe_mismatch(arg_matches, kwarg_matches):
            args_desc = map(describe_arg, self._allowed_args, arg_matches)
            kwargs_desc = describe_kwargs(kwarg_matches)
            return function_call_str_multiple_lines(self._name, args_desc, kwargs_desc)
            
        if len(self._allowed_args) != len(args):
            mismatch_description.append("%s [wrong number of positional arguments]" % str(self))
            return False
            
        missing_kwargs = set(self._allowed_kwargs.keys()) - set(kwargs.keys())
        if len(missing_kwargs) > 0:
            mismatch_description.append("%s [missing keyword arguments: %s]" % (str(self), ", ".join(sorted(missing_kwargs))))
            return False
            
        extra_kwargs = set(kwargs.keys()) - set(self._allowed_kwargs.keys())
        if len(extra_kwargs) > 0:
            mismatch_description.append("%s [unexpected keyword arguments: %s]" % (str(self), ", ".join(extra_kwargs)))
            return False
        
        arg_matches = [
            matcher.match(arg)
            for matcher, arg in zip(self._allowed_args, args)
        ]
        kwarg_matches = [
            (key, matcher.match(kwargs[key]))
            for key, matcher in iteritems(self._allowed_kwargs)
        ]
        matches = arg_matches + [match for key, match in kwarg_matches]
        
        if not all(match.is_match for match in matches):
            mismatch_description.append(describe_mismatch(arg_matches, kwarg_matches))
            return False
                
        return True
    
    def __call__(self, *args, **kwargs):
        if self._call_count.none_remaining():
            raise FunkyError("Cannot call any more times")
        if not self.accepts(args, kwargs, []):
            raise FunkyError("Called with wrong arguments")
        self._call_count.decrement()
        for sequence in self._sequences:
            sequence.add_actual_call(self)
        return self._action()
    
    def with_args(self, *args, **kwargs):
        self._arguments_set = True
        self._allowed_args = tuple(map(to_matcher, args))
        self._allowed_kwargs = dict([(key, to_matcher(kwargs[key])) for key in kwargs])
        return self
    
    def returns(self, return_value):
        self._action = lambda: return_value
        return self

    def raises(self, error):
        def action():
            raise error
        self._action = action
        return self

    def in_sequence(self, sequence):
        self._sequences.append(sequence)
        sequence.add_expected_call(self)
        return self

    def is_satisfied(self):
        return self._call_count.is_satisfied()

    def __str__(self):
        if self._arguments_set:
            return function_call_str(
                self._name,
                [arg.describe() for arg in self._allowed_args],
                map_values(lambda arg: arg.describe(), self._allowed_kwargs),
            )
        return self._name
