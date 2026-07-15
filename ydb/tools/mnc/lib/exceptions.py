class CliError(Exception):
    def __init__(self, message, result=None):
        super().__init__(message)
        self.result = result


class ConfigError(Exception):
    def __init__(self, config_path, message):
        super().__init__(message)
        self.config_path = config_path


class SchemeError(Exception):
    def __init__(self, message):
        super().__init__(message)
