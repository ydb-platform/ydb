from __future__ import unicode_literals

import sys


def result(return_code, allow_error, output, stderr_output):
    result = ExecutionResult(return_code, output, stderr_output)
    if return_code == 0 or allow_error:
        return result
    else:
        raise result.to_error()
        

class ExecutionResult(object):
    def __init__(self, return_code, output, stderr_output):
        self.return_code = return_code
        self.output = output
        self.stderr_output = stderr_output
        
    def to_error(self):
        return RunProcessError(
            self.return_code,
            self.output, 
            self.stderr_output
        )
        
        
class RunProcessError(RuntimeError):
    def __init__(self, return_code, output, stderr_output):
        message = "return code: {0}\noutput:{1}\nstderr output:{2}".format(
            return_code, _render_output(output), _render_output(stderr_output))
        super(type(self), self).__init__(message)
        self.return_code = return_code
        self.output = output
        self.stderr_output = stderr_output


def _render_output(output):
    if isinstance(output, unicode):
        return "\n" + output
    else:
        result =  repr(output)
        if result.startswith("b"):
            return " " + result
        else:
            return " b" + result

if sys.version_info[0] >= 3:
    unicode = str
