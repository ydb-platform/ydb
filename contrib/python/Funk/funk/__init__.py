from functools import wraps

from funk.error import FunkyError
from funk.call import Call
from funk.call import IntegerCallCount
from funk.call import InfiniteCallCount
from funk.sequence import Sequence
from funk.util import function_call_str
from . import pycompat
from .tools import data


__all__ = ['with_mocks', 'Mocks', 'expects', 'allows', 'expects_call', 'allows_call', 'data']

class UnexpectedInvocationError(AssertionError):
    def __init__(self, mock_name, args, kwargs, expectations):
        args_str = map(repr, args)
        kwargs_str = {}
        for key, value in pycompat.iteritems(kwargs):
            kwargs_str[key] = repr(value)
        call_str = function_call_str(mock_name, args_str, kwargs_str)
        exception_str = ["Unexpected invocation: %s" % call_str]
        exception_str.append("\nThe following expectations on %s did not match:\n    " % mock_name)
        if len(expectations) > 0:
            exception_str.append("\n    ".join(e.replace("\n", "\n    ") for e in expectations))
        else:
            exception_str.append("No expectations set.")
        super(UnexpectedInvocationError, self).__init__(''.join(exception_str))

class Mock(object):
    def __init__(self, base, name):
        self._mocked_calls = MockedCalls(base, name)
        self._base = base
        
    def __getattribute__(self, name):
        my = lambda name: object.__getattribute__(self, name)
        mocked_calls = my('_mocked_calls')
        base = my('_base')
        if name in mocked_calls or (base is not None and hasattr(base, name)):
            return mocked_calls.for_method(name)
        return my(name)
        
    def __call__(self, *args, **kwargs):
        return object.__getattribute__(self, "_mocked_calls").for_self()(*args, **kwargs)
        
    def _verify(self):
        object.__getattribute__(self, "_mocked_calls").verify()

class MockedCalls(object):
    def __init__(self, base, mock_name):
        self._base = base
        self._method_calls = {}
        self._function_calls = []
        self._mock_name = mock_name
    
    def add_method_call(self, method_name, call_count):
        if self._base is not None:
            if not hasattr(self._base, method_name):
                raise AssertionError("Method '%s' is not defined on type object '%s'" % (method_name, self._base.__name__))
            if not callable(getattr(self._base, method_name)):
                raise AssertionError("Attribute '%s' is not callable on type object '%s'" % (method_name, self._base.__name__))
        call = Call("%s.%s" % (self._mock_name, method_name), call_count)
        
        if method_name not in self._method_calls:
            self._method_calls[method_name] = []
        
        self._method_calls[method_name].append(call)
        return call
    
    def add_function_call(self, call_count):
        call = Call(self._mock_name, call_count)
        self._function_calls.append(call)
        return call
    
    def for_method(self, name):
        return MockedCallsForFunction("%s.%s" % (self._mock_name, name), self._method_calls.get(name, []))
    
    def for_self(self):
        return MockedCallsForFunction(self._mock_name, self._function_calls)
    
    def __contains__(self, name):
        return name in self._method_calls
        
    def verify(self):
        for method_name in self._method_calls:
            for call in self._method_calls[method_name]:
                self._verify_call(call)
        for call in self._function_calls:
            self._verify_call(call)
                
    def _verify_call(self, call):
        if not call.is_satisfied():
            raise AssertionError("Not all expectations were satisfied. Expected call: %s" % call)

class MockedCallsForFunction(object):
    def __init__(self, name, calls):
        self._name = name
        self._calls = calls
        
    def __call__(self, *args, **kwargs):
        desc = []
        for call in self._calls:
            if call.accepts(args, kwargs, desc):
                return call(*args, **kwargs)
        
        raise UnexpectedInvocationError(self._name, args, kwargs, desc)

def with_mocks(test_function, mock_factory=None):
    @wraps(test_function)
    def test_function_with_mocks(*args, **kwargs):
        if 'mocks' in kwargs:
            raise FunkyError("mocks has already been set")
        mocks = Mocks(mock_factory)
        kwargs['mocks'] = mocks
        test_function(*args, **kwargs)
        mocks.verify()
    
    return test_function_with_mocks

class MethodArgumentsSetter(object):
    def __init__(self, call):
        self._call = call
    
    def __call__(self, *args, **kwargs):
        return self._call.with_args(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._call, name)

class ExpectationCreator(object):
    def __init__(self, expectation_setter):
        self._expectation_setter = expectation_setter
    
    def __getattribute__(self, name):
        my = lambda name: object.__getattribute__(self, name)
        return MethodArgumentsSetter(my('_expectation_setter')(name))

def expects(mock, method_name=None):
    if method_name is None:
        return ExpectationCreator(lambda method_name: expects(mock, method_name))
    return object.__getattribute__(mock, "_mocked_calls").add_method_call(method_name, IntegerCallCount(1))

def allows(mock, method_name=None):
    if method_name is None:
        return ExpectationCreator(lambda method_name: allows(mock, method_name))
    return object.__getattribute__(mock, "_mocked_calls").add_method_call(method_name, InfiniteCallCount())
    
def expects_call(mock):
    return MethodArgumentsSetter(object.__getattribute__(mock, "_mocked_calls").add_function_call(IntegerCallCount(1)))

def allows_call(mock):
    return MethodArgumentsSetter(object.__getattribute__(mock, "_mocked_calls").add_function_call(InfiniteCallCount()))

class Mocks(object):
    def __init__(self, mock_factory=None):
        if mock_factory is None:
            mock_factory = Mock
        self._mocks = []
        self._mock_factory = mock_factory
        
        for attr in ["allows", "expects", "data"]:
            setattr(self, attr, globals()[attr])
    
    def mock(self, base=None, name=None):
        mock = self._mock_factory(base, self._generate_name(name, base))
        self._mocks.append(mock)
        return mock
        
    def verify(self):
        for mock in self._mocks:
            mock._verify()
            
    def sequence(self):
        return Sequence()
        
    def _generate_name(self, name, base):
        if name is not None:
            return name
        if base is None:
            return "unnamed"
        name = []
        name.append(base.__name__[0].lower())
        for character in base.__name__[1:]:
            if character.isupper():
                name.append("_%s" % character.lower())
            else:
                name.append(character)
        return ''.join(name)
