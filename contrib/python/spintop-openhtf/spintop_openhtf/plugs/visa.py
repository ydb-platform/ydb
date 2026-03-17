
from .base import UnboundPlug


import os
import time

def bytes_from_file(filename, chunksize=8192):
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(chunksize)
            if chunk:
                for b in chunk:
                    yield b
            else:
                break

class VISAException(Exception):
    pass

class VISAInterface(UnboundPlug):
    def __init__(self, instrument=None, open_timeout=5, timeout=5):
        super().__init__()
        self.instr_name = instrument
        self.open_timeout = open_timeout*1000 # ms
        self.timeout = timeout*1000 # ms
        self.instr = None
        self.command_queue = []

    # Old opentest "Server" Side Interface

    def open(self, _instrument=None):
        self.close()
        if not _instrument:
            import pyvisa
            self.logger.info("Connecting to instrument [%s]", self.instr_name)
            try:
                resource_manager = pyvisa.ResourceManager()
            except OSError as e:
                raise VISAException("Please install NI-VISA driver on this computer in order to use the VISA Service.") from e
            _instrument = resource_manager.open_resource(self.instr_name, open_timeout=self.open_timeout)
            _instrument.timeout = self.timeout

        self.instr = _instrument

    def close(self):
        if self.instr is not None:
            self.instr.close()
            self.instr = None

    def visa_client_connected(function):
        def check_connected(self, *args, **kwargs):
            if self.instr is None:
                self.logger.info("Connection is no longer alive")
                self.open()
            return function(self, *args, **kwargs)

        return check_connected

    @visa_client_connected
    def _execute_request(self, 
            multiple_commands=[], 
            command="", 
            read_binary=False,
            file_name="",
            doread=False,
            unstack_errors=False,
            timeout=None
        ):

        if timeout is None: timeout = self.timeout

        self.logger.debug("Timeout is %s seconds" % timeout)
        timeout = timeout*1000 # to ms
        self.instr.timeout = timeout
        output = ""
        if file_name != "" and command != "":
            if read_binary:
                self.logger.info('Copying remote file to %s' % file_name)
                values = self.instr.query_binary_values(command, datatype='c')
                values = bytearray(values)
                with open(file_name, 'wb') as f:
                    f.write(values)
            else:
                #Command + binary file
                binary = bytes_from_file(file_name)
                self.logger.info("Sending binary file: %s<%s>" % (command, file_name))
                self.instr.write_binary_values(command, list(binary), datatype='c')
        else:
            #Command only
            if command != "":
                multiple_commands = [command] + multiple_commands

            output = self._try_command_x_times(self._send_commands, 3, multiple_commands, doread)

        if unstack_errors:
            self.assert_no_errors()

        return output

    def _send_commands(self, multiple_commands, doread):
        output = ''
        for cmd in multiple_commands:
            self.logger.info("Sending command: %s", cmd)
            self._try_command_x_times(self.instr.write, 3, cmd)

        try:
            if doread:
                output = self.instr.read()
                self.logger.info("Read: %s", output.strip())
            else:
                #If not expected to read, block until all operations are completed
                self.logger.debug("Waiting for operations to finish...")
                output = self.instr.query("*OPC?")
        except Exception as e:
            self.logger.info(e)
            self.logger.warning("Intrument read failed")
            self.logger.debug("Unstacking all errors from instrument")
            raise VISAException(self._format_get_errors_read())

        return output.strip()

    def _try_command_x_times(self, fn, tries, *args, **kwargs):
        while True:
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                tries = tries - 1
                if tries <= 0:
                    raise
                self.logger.warning('%s: Retrying' % str(e))

    def assert_no_errors(self):
        errors = self._get_errors()
        if len(errors) > 0:
            raise VISAException(self._format_get_errors_read(errors))

    def _format_get_errors_read(self, errors=None):
        if not errors:
            errors = self._get_errors()
        if len(errors) <= 0:
            string = "No errors but read failed: was there something to be read?"
        else:
            string = "ERRORS"
            for error in errors:
                string = string + ":" + error

        return string

    def _get_errors(self):
        ERROR_QUERY = "SYST:ERR?"
        errors = []
        error_string = self.instr.query(ERROR_QUERY)
        self.logger.debug("%s", error_string.strip())
        code, msg = error_string.split(',')
        code = code.strip()
        msg = msg.strip()
        if int(code) != 0:
            errors = errors + [msg]
            errors = errors + self._get_errors()

        return errors

    # Old opentest "Client" Side Interface

    def execute_command(self, command, doread=False, unstack_errors=False, timeout=None):
        return self._execute_request(command=command, doread=doread, unstack_errors=unstack_errors, timeout=timeout)

    def write_binary_file(self, command, file_name, **kwargs):
        output = self._execute_request(command=command, file_name=file_name, **kwargs)

    def read_binary_file(self, command, file_name, **kwargs):
        output = self._execute_request(command=command, file_name=file_name, read_binary=True, **kwargs)

    def queue_command(self, command):
        self.command_queue = self.command_queue + [command]

    def execute_queued_commands(self, **kwargs):
        try:
            output = self._execute_request(multiple_commands=self.command_queue, **kwargs)
        finally:
            self.command_queue = []
        return output

    def set_and_read_command(self, command, set_value):
        set_cmd = command + " " + str(set_value)
        read_cmd = command + "?"
        return self.execute_command(set_cmd + ";" + read_cmd, doread = True)

    def read(self):
        return self._execute_request(doread=True)

    def read_error(self):
        return self._execute_request(unstack_errors=True)

    def _parse_float_output(self, output):
        values_string = output.split(',')
        values = ()
        for value_str in values_string:
            values = values + (float(value_str),)
        return values

    def reset(self):
        self.execute_command('*RST', timeout=10.0)
        self.instrument_reset()

    def instrument_reset(self):
        pass
