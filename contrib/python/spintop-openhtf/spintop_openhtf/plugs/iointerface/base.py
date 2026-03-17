import time
import threading
from queue import Queue, Empty
from asyncio import Protocol

from io import StringIO

from ..base import UnboundPlug

class IOTargetTimeout(Exception):
    pass

class IOInterface(UnboundPlug, Protocol):
    """An interface to a read/write interface.
    
    Allows reading and writing. A background thread reads any data that comes in 
    and those lines can be accessed using the `next_line` function.
    
    """

    def __init__(self):
        super().__init__()
        self._line_buffer = ""
        self._read_lines = Queue()
        self._timeout_timer = None
        self._last_receive_time = None

        self.timeout = 0.5
        self.eol = "\n"

    def tearDown(self):
        """Tear down the plug instance."""
        self.close()
        self.logger.close_log()

    def open(self, *args, **kwargs):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def write(self, string):
        """Write the string into the io interface."""
        self.logger.debug("> {!r}".format(string))
        return self._write(string)

    def _write(self, string):
        raise NotImplementedError()

    def data_received(self, data):
        if self._timeout_timer is not None:
            self._timeout_timer.cancel()
        
        force_clear = False
        if self.check_receive_delta() and data is None and self._line_buffer:
            force_clear = True

        data_to_queue = None

        if not data:
            data = b''

        string = data.decode('utf8')
        if force_clear or string.endswith(self.eol) or (self._line_buffer + string).endswith(self.eol):
            data_to_queue = self._line_buffer + string
            self._line_buffer = ""
        else:
            self._line_buffer += string
        
        self._timeout_timer = threading.Timer(self.timeout, self.no_data_received)
        self._timeout_timer.start()

        if data_to_queue:
            strings_with_eol = data_to_queue.split(self.eol)
            # Last has no eol, all others do.
            for index, line in enumerate(strings_with_eol):
                if index < len(strings_with_eol) - 1:
                    line = line + self.eol

                if line:
                    self.logger.debug(line)
                    self._read_lines.put(line)

    def no_data_received(self):
        return self.data_received(None)

    def check_receive_delta(self):
        if self._last_receive_time:
            delta = time.time() - self._last_receive_time
        else:
            delta = 0

        self._last_receive_time = time.time()

        return delta > self.timeout

    def keep_lines(self, lines_to_keep):
        """Clear all lines in the buffer except the last lines_to_keep lines."""
        current_lines = self._read_lines.qsize()
        while current_lines > lines_to_keep:
            current_lines = current_lines - 1
            self.next_line()
            current_lines = self._read_lines.qsize()
        
    def clear_lines(self):
        """Clear all lines in the buffer."""
        self._read_lines.queue.clear()

    def next_line(self, timeout=10):
        """ Waits up to timeout seconds and return the next line available in the buffer.
        """
        try:
            return self._read_lines.get(timeout=timeout)
        except Empty:
            return None

    def eof_received(self):
        pass
    
    def message_target(self, message, target, timeout=None, keeplines=0, _timeout_raises=True):
        """Sends the message string and waits for any string in target to be received. 
        
        Parameters:
            message: The string to write into the io interface
            target: A string or a list of string to check for in the read lines.
            timeout: (default=None, no timeout) Wait up to this seconds for the targets. If busted, this will raise a IOTargetTimeout.
            keeplines: (default=0, discard all) Before sending the message, call self.keep_lines with this value.
            _timeout_raises: (default=True) If True, a timeout will raise an error.
        """
        self.keep_lines(keeplines)
        self.write(message)

        if isinstance(target, str):
            targets = [target]
        else:
            targets = target

        start_time = time.time()
        target_found = None
        contentstring = StringIO()

        while not target_found and (timeout is None or time.time() - start_time < timeout):
            line = self.next_line(timeout=timeout) # wait no more than the timeout
            if line:
                contentstring.write(line)
                for targetstr in targets:
                    if targetstr in line:
                        target_found = targetstr
                        break

        if target_found is None:
            log_message = 'Message Target: timeout occured while waiting for %s' % str(targets)
            if _timeout_raises:
                raise IOTargetTimeout(log_message)
        else:
            log_message = 'Message Target: Found %s' % target_found

        contentlog = contentstring.getvalue()
        contentstring.close()

        return contentlog
    
    def execute_command(self, command=None, timeout=None, target=None):
        """Adds the self.eol to command and call message_target using either
        target as target or self._target if defined. This is used for executing
        commands in a shell-like environment and to wait for the prompt.
        self._target or target should be the expected prompt."""
        if target is None:
            target = getattr(self, '_target', '$')
        command = command + self.eol
        return_value = self.message_target(command, target, timeout)
        return return_value
