class Sequence(object):
    def __init__(self):
        self._expected_calls = []
    
    def add_expected_call(self, call):
        self._expected_calls.append(call)
        
    def add_actual_call(self, call):
        while len(self._expected_calls) and call is not self._expected_calls[0] and self._expected_calls[0].is_satisfied():
            self._expected_calls.pop(0)
        if not self._expected_calls:
            raise AssertionError("Invocation out of order. Expected no more calls in sequence, but got %s." % call)
        if call is not self._expected_calls[0]:
            raise AssertionError("Invocation out of order. Expected %s, but got %s." % (self._expected_calls[0], call))
        

