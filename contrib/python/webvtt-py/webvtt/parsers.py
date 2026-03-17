import re
import os
import codecs

from .errors import MalformedFileError, MalformedCaptionError
from .structures import Block, Style, Caption


class TextBasedParser(object):
    """
    Parser for plain text caption files.
    This is a generic class, do not use directly.
    """

    TIMEFRAME_LINE_PATTERN = ''
    PARSER_OPTIONS = {}

    def __init__(self, parse_options=None):
        self.captions = []
        self.parse_options = parse_options or {}

    def read(self, file):
        """Reads the captions file."""
        content = self._get_content_from_file(file_path=file)
        self._validate(content)
        self._parse(content)

        return self

    def read_from_buffer(self, buffer):
        content = self._read_content_lines(buffer)
        self._validate(content)
        self._parse(content)

        return self

    def _get_content_from_file(self, file_path):
        encoding = self._read_file_encoding(file_path)
        with open(file_path, encoding=encoding) as f:
            return self._read_content_lines(f)

    def _read_file_encoding(self, file_path):
        first_bytes = min(32, os.path.getsize(file_path))
        with open(file_path, 'rb') as f:
            raw = f.read(first_bytes)

        if raw.startswith(codecs.BOM_UTF8):
            return 'utf-8-sig'
        else:
            return 'utf-8'

    def _read_content_lines(self, file_obj):

        lines = [line.rstrip('\n\r') for line in file_obj.readlines()]

        if not lines:
            raise MalformedFileError('The file is empty.')

        return lines

    def _read_content(self, file):
        return self._get_content_from_file(file_path=file)

    def _parse_timeframe_line(self, line):
        """Parse timeframe line and return start and end timestamps."""
        tf = self._validate_timeframe_line(line)
        if not tf:
            raise MalformedCaptionError('Invalid time format')

        return tf.group(1), tf.group(2)

    def _validate_timeframe_line(self, line):
        return re.match(self.TIMEFRAME_LINE_PATTERN, line)

    def _is_timeframe_line(self, line):
        """
        This method returns True if the line contains the timeframes.
        To be implemented by child classes.
        """
        raise NotImplementedError

    def _validate(self, lines):
        """
        Validates the format of the parsed file.
        To be implemented by child classes.
        """
        raise NotImplementedError

    def _should_skip_line(self, line, index, caption):
        """
        This method returns True for a line that should be skipped.
        Implement in child classes if needed.
        """
        return False

    def _parse(self, lines):
        self.captions = []
        c = None

        for index, line in enumerate(lines):
            if self._is_timeframe_line(line):
                try:
                    start, end = self._parse_timeframe_line(line)
                except MalformedCaptionError as e:
                    raise MalformedCaptionError('{} in line {}'.format(e, index + 1))
                c = Caption(start, end)
            elif self._should_skip_line(line, index, c):  # allow child classes to skip lines based on the content
                continue
            elif line:
                if c is None:
                    raise MalformedCaptionError(
                        'Caption missing timeframe in line {}.'.format(index + 1))
                else:
                    c.add_line(line)
            else:
                if c is None:
                    continue
                if not c.lines:
                    if self.PARSER_OPTIONS.get('ignore_empty_captions', False):
                        c = None
                        continue
                    raise MalformedCaptionError('Caption missing text in line {}.'.format(index + 1))

                self.captions.append(c)
                c = None

        if c is not None and c.lines:
            self.captions.append(c)


class SRTParser(TextBasedParser):
    """
    SRT parser.
    """

    TIMEFRAME_LINE_PATTERN = re.compile(r'\s*(\d+:\d{2}:\d{2},\d{3})\s*-->\s*(\d+:\d{2}:\d{2},\d{3})')

    PARSER_OPTIONS = {
        'ignore_empty_captions': True
    }

    def _validate(self, lines):
        if len(lines) < 2 or lines[0] != '1' or not self._validate_timeframe_line(lines[1]):
            raise MalformedFileError('The file does not have a valid format.')

    def _is_timeframe_line(self, line):
        return '-->' in line

    def _should_skip_line(self, line, index, caption):
        return caption is None and line.isdigit()


class WebVTTParser(TextBasedParser):
    """
    WebVTT parser.
    """

    TIMEFRAME_LINE_PATTERN = re.compile(r'\s*((?:\d+:)?\d{2}:\d{2}.\d{3})\s*-->\s*((?:\d+:)?\d{2}:\d{2}.\d{3})')
    COMMENT_PATTERN = re.compile(r'NOTE(?:\s.+|$)')
    STYLE_PATTERN = re.compile(r'STYLE[ \t]*$')

    def __init__(self):
        super().__init__()
        self.styles = []

    def _compute_blocks(self, lines):
        blocks = []

        for index, line in enumerate(lines, start=1):
            if line:
                if not blocks:
                    blocks.append(Block(index))
                if not blocks[-1].lines:
                    if not line.strip():
                        continue
                    blocks[-1].line_number = index
                blocks[-1].lines.append(line)
            else:
                blocks.append(Block(index))

        # filter out empty blocks and skip signature
        return list(filter(lambda x: x.lines, blocks))[1:]

    def _parse_cue_block(self, block):
        caption = Caption()
        cue_timings = None
        additional_blocks = None

        for line_number, line in enumerate(block.lines):
            if self._is_cue_timings_line(line):
                if cue_timings is None:
                    try:
                        cue_timings = self._parse_timeframe_line(line)
                    except MalformedCaptionError as e:
                        raise MalformedCaptionError(
                            '{} in line {}'.format(e, block.line_number + line_number))
                else:
                    additional_blocks = self._compute_blocks(
                        ['WEBVTT', ''] + block.lines[line_number:]
                    )
                    break
            elif line_number == 0:
                caption.identifier = line
            else:
                caption.add_line(line)

        caption.start = cue_timings[0]
        caption.end = cue_timings[1]
        return caption, additional_blocks

    def _parse(self, lines):
        self.captions = []
        blocks = self._compute_blocks(lines)
        self._parse_blocks(blocks)

    def _is_empty(self, block):
        is_empty = True

        for line in block.lines:
            if line.strip() != "":
                is_empty = False

        return is_empty

    def _parse_blocks(self, blocks):
        for block in blocks:
            # skip empty blocks
            if self._is_empty(block):
                continue

            if self._is_cue_block(block):
                caption, additional_blocks = self._parse_cue_block(block)
                self.captions.append(caption)

                if additional_blocks:
                    self._parse_blocks(additional_blocks)

            elif self._is_comment_block(block):
                continue
            elif self._is_style_block(block):
                if self.captions:
                    raise MalformedFileError(
                        'Style block defined after the first cue in line {}.'
                        .format(block.line_number))
                style = Style()
                style.lines = block.lines[1:]
                self.styles.append(style)
            else:
                if len(block.lines) == 1:
                    raise MalformedCaptionError(
                        'Standalone cue identifier in line {}.'.format(block.line_number))
                else:
                    raise MalformedCaptionError(
                        'Missing timing cue in line {}.'.format(block.line_number+1))

    def _validate(self, lines):
        if not re.match('WEBVTT', lines[0]):
            raise MalformedFileError('The file does not have a valid format')

    def _is_cue_timings_line(self, line):
        return '-->' in line

    def _is_cue_block(self, block):
        """Returns True if it is a cue block
        (one of the two first lines being a cue timing line)"""
        return any(map(self._is_cue_timings_line, block.lines[:2]))

    def _is_comment_block(self, block):
        """Returns True if it is a comment block"""
        return re.match(self.COMMENT_PATTERN, block.lines[0])

    def _is_style_block(self, block):
        """Returns True if it is a style block"""
        return re.match(self.STYLE_PATTERN, block.lines[0])


class SBVParser(TextBasedParser):
    """
    YouTube SBV parser.
    """

    TIMEFRAME_LINE_PATTERN = re.compile(r'\s*(\d+:\d{2}:\d{2}.\d{3}),(\d+:\d{2}:\d{2}.\d{3})')

    PARSER_OPTIONS = {
        'ignore_empty_captions': True
    }

    def _validate(self, lines):
        if not self._validate_timeframe_line(lines[0]):
            raise MalformedFileError('The file does not have a valid format')

    def _is_timeframe_line(self, line):
        return self._validate_timeframe_line(line)
