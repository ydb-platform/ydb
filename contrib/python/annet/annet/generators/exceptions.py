class GeneratorError(Exception):
    pass


class InvalidValueFromGenerator(ValueError):
    pass


class NotSupportedDevice(GeneratorError):
    pass
