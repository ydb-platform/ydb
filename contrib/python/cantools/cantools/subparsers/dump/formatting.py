import textwrap

from ...database.utils import start_bit


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def signal_tree_string(message, console_width=80, with_comments=False):
    """Returns the message signal tree as a string.

    """

    def get_prefix(index, length):
        if index < length - 1:
            return '|   '
        else:
            return '    '

    def add_prefix(prefix, lines):
        return [prefix + line for line in lines]

    def format_signal_line(signal_name):
        siginst = message.get_signal_by_name(signal_name)
        signal_name_line = signal_name

        if with_comments:
            com = []
            if siginst.comment:
                com.append(siginst.comment)
            if siginst.unit:
                com.append(f'[{siginst.unit}]')

            comstr = ' '.join(com)
            if len(comstr) > 0:
                signal_name_line = f'{signal_name} {Colors.OKBLUE}{comstr}{Colors.ENDC}'

        signal_name_line = textwrap.wrap(signal_name_line, width=console_width - 2, initial_indent='+-- ',
                                         subsequent_indent=(' ' * (8 + len(signal_name))))
        signal_name_line = '\n'.join(signal_name_line)

        return signal_name_line

    def format_mux(mux):
        signal_name, multiplexed_signals = next(iter(mux.items()))
        selector_signal = message.get_signal_by_name(signal_name)
        multiplexed_signals = sorted(multiplexed_signals.items())
        lines = []

        for index, multiplexed_signal in enumerate(multiplexed_signals):
            multiplexer_id, signal_names = multiplexed_signal
            multiplexer_desc = f'{multiplexer_id}'

            if selector_signal.choices and \
               multiplexer_id in selector_signal.choices:
                multiplexer_desc = \
                    f'{selector_signal.choices[multiplexer_id]} ' \
                    f'({multiplexer_id})'

            lines.append(f'+-- {multiplexer_desc}')
            lines += add_prefix(get_prefix(index, len(multiplexed_signals)),
                                format_level_lines(signal_names))

        return format_signal_line(signal_name), lines

    def format_level_lines(signal_names):
        lines = []

        for index, signal_name in enumerate(signal_names):
            if isinstance(signal_name, dict):
                signal_name_line, signal_lines = format_mux(signal_name)
                signal_lines = add_prefix(get_prefix(index, len(signal_names)),
                                          signal_lines)
            else:
                signal_name_line = format_signal_line(signal_name)
                signal_lines = []

            lines.append(signal_name_line)
            lines += signal_lines

        return lines

    lines = format_level_lines(message.signal_tree)
    lines = ['-- {root}', *add_prefix('   ', lines)]

    return '\n'.join(lines)


def layout_string(message, signal_names=True):
    """Returns the message layout as an ASCII art string. Each signal is
    an arrow from LSB ``x`` to MSB ``<``. Overlapping signal bits
    are set to ``X``.

    Set `signal_names` to ``False`` to hide signal names.

    .. code:: text

                            Bit

                7   6   5   4   3   2   1   0
                +---+---+---+---+---+---+---+---+
            0 |   |   |   |   |   |<----------|
                +---+---+---+---+---+---+---+---+
            1 |------x|   |   |   |   |<-x|   |
                +---+---+---+---+---+---+---+---+
                    |                   +-- Bar
                    +-- Foo
                +---+---+---+---+---+---+---+---+
            2 |   |   |   |   |   |   |   |   |
        B     +---+---+---+---+---+---+---+---+
        y   3 |----XXXXXXX---x|   |   |   |   |
        t     +---+---+---+---+---+---+---+---+
        e           +-- Fie
                +---+---+---+---+---+---+---+---+
            4 |-------------------------------|
                +---+---+---+---+---+---+---+---+
            5 |   |   |<----------------------|
                +---+---+---+---+---+---+---+---+
                        +-- Fum
                +---+---+---+---+---+---+---+---+
            6 |   |   |   |   |   |   |   |   |
                +---+---+---+---+---+---+---+---+
            7 |   |   |   |   |   |   |   |   |
                +---+---+---+---+---+---+---+---+

    """

    def format_big():
        signals = []

        for signal in message._signals:
            if signal.byte_order != 'big_endian':
                continue

            formatted = start_bit(signal) * '   '
            formatted += '<{}x'.format((3 * signal.length - 2) * '-')
            signals.append(formatted)

        return signals

    def format_little():
        signals = []

        for signal in message._signals:
            if signal.byte_order != 'little_endian':
                continue

            formatted = signal.start * '   '
            formatted += 'x{}<'.format((3 * signal.length - 2) * '-')
            end = signal.start + signal.length

            if end % 8 != 0:
                formatted += (8 - (end % 8)) * '   '

            formatted = ''.join([
                formatted[i:i + 24][::-1]
                for i in range(0, len(formatted), 24)
            ])
            signals.append(formatted)

        return signals

    def format_byte_lines():
        # Signal lines.
        signals = format_big() + format_little()

        if len(signals) > 0:
            length = max([len(signal) for signal in signals])

            if length % 24 != 0:
                length += (24 - (length % 24))

            signals = [signal + (length - len(signal)) * ' ' for signal in signals]

        # Signals union line.
        signals_union = ''

        for chars in zip(*signals, strict=False):
            head = chars.count('<')
            dash = chars.count('-')
            tail = chars.count('x')

            if head + dash + tail > 1:
                signals_union += 'X'
            elif head == 1:
                signals_union += '<'
            elif dash == 1:
                signals_union += '-'
            elif tail == 1:
                signals_union += 'x'
            else:
                signals_union += ' '

        # Split the signals union line into byte lines, 8 bits per
        # line.
        byte_lines = [
            signals_union[i:i + 24]
            for i in range(0, len(signals_union), 24)
        ]

        unused_byte_lines = (message._length - len(byte_lines))

        if unused_byte_lines > 0:
            byte_lines += unused_byte_lines * [24 * ' ']

        # Insert bits separators into each byte line.
        lines = []

        for byte_line in byte_lines:
            line = ''
            prev_byte = None

            for i in range(0, 24, 3):
                byte_triple = byte_line[i:i + 3]

                if i == 0:
                    line += '|'
                elif byte_triple[0] in ' <>x':
                    line += '|'
                elif byte_triple[0] == 'X':
                    if prev_byte == 'X':
                        line += 'X'
                    elif prev_byte == '-':
                        line += '-'
                    else:
                        line += '|'
                else:
                    line += '-'

                line += byte_triple
                prev_byte = byte_triple[2]

            line += '|'
            lines.append(line)

        # Add byte numbering.
        number_width = len(str(len(lines))) + 4
        number_fmt = f'{{:{number_width - 1}d}} {{}}'
        a = []

        for number, line in enumerate(lines):
            a.append(number_fmt.format(number, line))

        return a, len(lines), number_width

    def add_header_lines(lines, number_width):
        padding = number_width * ' '

        return [
            padding + '               Bit',
            padding + '',
            padding + '  7   6   5   4   3   2   1   0',
            padding + '+---+---+---+---+---+---+---+---+',
            *lines,
        ]

    def add_horizontal_lines(byte_lines, number_width):
        padding = number_width * ' '
        lines = []

        for byte_line in byte_lines:
            lines.append(byte_line)
            lines.append(padding + '+---+---+---+---+---+---+---+---+')

        return lines

    def name_bit(signal):
        offset = start_bit(signal) + signal.length - 1

        if signal.byte_order == 'big_endian':
            return (8 * (offset // 8) + (7 - (offset % 8)))
        else:
            return offset

    def add_signal_names(input_lines,
                         number_of_bytes,
                         number_width):
        # Find MSB and name of all signals.
        padding = number_width * ' '
        signals_per_byte = [[] for _ in range(number_of_bytes)]

        for signal in message._signals:
            byte, bit = divmod(name_bit(signal), 8)
            signals_per_byte[byte].append((bit, '+-- ' + signal.name))

        # Format signal lines.
        signal_lines_per_byte = []

        for signals in signals_per_byte:
            signals = sorted(signals)
            signals_lines = []

            for signal in signals:
                line = number_width * ' ' + '  ' + signal[1]
                line = (7 - signal[0]) * '    ' + line
                chars = list(line)

                for other_signal in signals:
                    if other_signal[0] > signal[0]:
                        other_signal_msb = (number_width
                                            + 2
                                            + 4 * (7 - other_signal[0]))
                        chars[other_signal_msb] = '|'

                signals_lines.append(''.join(chars))

            signal_lines_per_byte.append(signals_lines)

        # Insert the signals names lines among other lines.
        lines = []

        for number in range(number_of_bytes):
            lines += input_lines[2 * number: 2 * number + 2]

            if signal_lines_per_byte[number]:
                lines += signal_lines_per_byte[number]

                if number + 1 < number_of_bytes:
                    lines.append(
                        padding + '+---+---+---+---+---+---+---+---+')

        return lines

    def add_y_axis_name(lines):
        number_of_matrix_lines = (len(lines) - 3)

        if number_of_matrix_lines < 5:
            lines += (5 - number_of_matrix_lines) * ['     ']

        start_index = 4 + ((number_of_matrix_lines - 4) // 2 - 1)

        start_index = max(start_index, 4)

        axis_lines = start_index * ['  ']
        axis_lines += [' B', ' y', ' t', ' e']
        axis_lines += (len(lines) - start_index - 4) * ['  ']

        return [
            axis_line + line
            for axis_line, line in zip(axis_lines, lines, strict=False)
        ]

    lines, number_of_bytes, number_width = format_byte_lines()
    lines = add_horizontal_lines(lines, number_width)

    if signal_names:
        lines = add_signal_names(lines,
                                 number_of_bytes,
                                 number_width)

    lines = add_header_lines(lines, number_width)
    lines = add_y_axis_name(lines)
    lines = [line.rstrip() for line in lines]

    return '\n'.join(lines)


def signal_choices_string(message):
    """Returns the signal choices as a string.

    """

    lines = []

    for signal in message._signals:
        if signal.choices:
            lines.append('')
            lines.append(signal.name)

            for value, text in sorted(signal.choices.items()):
                lines.append(f'    {value} {text}')

    return '\n'.join(lines)
