import argparse

__version__ = '0.12.0'


class Integer:

    def __init__(self, minimum=None, maximum=None):
        self.minimum = minimum
        self.maximum = maximum

    def __call__(self, string):
        value = int(string, 0)

        if self.minimum is not None and self.maximum is not None:
            if not self.minimum <= value <= self.maximum:
                raise argparse.ArgumentTypeError(
                    f'{string} is not in the range {self.minimum}..{self.maximum}')
        elif self.minimum is not None:
            if value < self.minimum:
                raise argparse.ArgumentTypeError(
                    f'{string} is not in the range {self.minimum}..inf')
        elif self.maximum is not None:
            if value > self.maximum:
                raise argparse.ArgumentTypeError(
                    f'{string} is not in the range -inf..{self.maximum}')

        return value

    def __repr__(self):
        return 'integer'


def parse_log_level(value):
    """Parse given set log level value.

    Examples:

    <logger-name>=<level>

    <logger-1-name>=<level-1>:<logger-2-name>=<level-2>

    <logger-1-name>,<logger-2-name>=<level>

    """

    result = []

    if value is None:
        return result

    for part in value.split(':'):
        if '=' in part:
            names, level = part.split('=')

            for name in names.split(','):
                result.append((name, level))
        else:
            # None for root logger.
            result.append((None, part))

    return result
