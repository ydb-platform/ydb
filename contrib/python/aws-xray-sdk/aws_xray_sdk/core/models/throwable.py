import copy
import os
import binascii
import logging

log = logging.getLogger(__name__)


class Throwable:
    """
    An object recording exception infomation under trace entity
    `cause` section. The information includes the stack trace,
    working directory and message from the original exception.
    """
    def __init__(self, exception, stack, remote=False):
        """
        :param Exception exception: the catched exception.
        :param list stack: the formatted stack trace gathered
            through `traceback` module.
        :param bool remote: If False it means it's a client error
            instead of a downstream service.
        """
        self.id = binascii.b2a_hex(os.urandom(8)).decode('utf-8')

        try:
            message = str(exception)
            # in case there is an exception cannot be converted to str
        except Exception:
            message = None

        # do not record non-string exception message
        if isinstance(message, str):
            self.message = message

        self.type = type(exception).__name__
        self.remote = remote

        try:
            self._normalize_stack_trace(stack)
        except Exception:
            self.stack = None
            log.warning("can not parse stack trace string, ignore stack field.")

        if exception:
            setattr(exception, '_recorded', True)
            setattr(exception, '_cause_id', self.id)
			
    def to_dict(self):  
        """
        Convert Throwable object to dict with required properties that
        have non-empty values. 
        """  
        throwable_dict = {}
        
        for key, value in vars(self).items():  
            if isinstance(value, bool) or value:
                throwable_dict[key] = value       
        
        return throwable_dict

    def _normalize_stack_trace(self, stack):
        if stack is None:
            return None

        self.stack = []

        for entry in stack:
            path = entry[0]
            line = entry[1]
            label = entry[2]
            if 'aws_xray_sdk/' in path:
                continue

            normalized = {}
            normalized['path'] = os.path.basename(path).replace('\"', ' ').strip()
            normalized['line'] = line
            normalized['label'] = label.strip()

            self.stack.append(normalized)
