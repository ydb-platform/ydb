import os

from .parsers import WebVTTParser, SRTParser, SBVParser
from .writers import WebVTTWriter, SRTWriter
from .errors import MissingFilenameError

__all__ = ['WebVTT']


class WebVTT(object):
    """
    Parse captions in WebVTT format and also from other formats like SRT.

    To read WebVTT:

        WebVTT().read('captions.vtt')

    For other formats like SRT, use from_[format in lower case]:

        WebVTT().from_srt('captions.srt')

    A list of all supported formats is available calling list_formats().
    """

    def __init__(self, file='', captions=None, styles=None):
        self.file = file
        self._captions = captions or []
        self._styles = styles

    def __len__(self):
        return len(self._captions)

    def __getitem__(self, index):
        return self._captions[index]

    def __repr__(self):
        return '<%(cls)s file=%(file)s>' % {
            'cls': self.__class__.__name__,
            'file': self.file
        }

    def __str__(self):
        return '\n'.join([str(c) for c in self._captions])

    @classmethod
    def from_srt(cls, file):
        """Reads captions from a file in SubRip format."""
        parser = SRTParser().read(file)
        return cls(file=file, captions=parser.captions)

    @classmethod
    def from_sbv(cls, file):
        """Reads captions from a file in YouTube SBV format."""
        parser = SBVParser().read(file)
        return cls(file=file, captions=parser.captions)

    @classmethod
    def read(cls, file):
        """Reads a WebVTT captions file."""
        parser = WebVTTParser().read(file)
        return cls(file=file, captions=parser.captions, styles=parser.styles)

    @classmethod
    def read_buffer(cls, buffer):
        """Reads a WebVTT captions from a file-like object.
        Such file-like object may be the return of an io.open call,
        io.StringIO object, tempfile.TemporaryFile object, etc."""
        parser = WebVTTParser().read_from_buffer(buffer)
        return cls(captions=parser.captions, styles=parser.styles)

    def _get_output_file(self, output, extension='vtt'):
        if not output:
            if not self.file:
                raise MissingFilenameError
            # saving an original vtt file will overwrite the file
            # and for files read from other formats will save as vtt
            # with the same name and location
            return os.path.splitext(self.file)[0] + '.' + extension
        else:
            target = os.path.join(os.getcwd(), output)
            if os.path.isdir(target):
                # if an output is provided and it is a directory
                # the file will be saved in that location with the same name
                filename = os.path.splitext(os.path.basename(self.file))[0]
                return os.path.join(target, '{}.{}'.format(filename, extension))
            else:
                if target[-3:].lower() != extension:
                    target += '.' + extension
                # otherwise the file will be written in the specified location
                return target

    def save(self, output=''):
        """Save the document.
        If no output is provided the file will be saved in the same location. Otherwise output
        can determine a target directory or file.
        """
        self.file = self._get_output_file(output)
        with open(self.file, 'w', encoding='utf-8') as f:
            self.write(f)

    def save_as_srt(self, output=''):
        self.file = self._get_output_file(output, extension='srt')
        with open(self.file, 'w', encoding='utf-8') as f:
            self.write(f, format='srt')

    def write(self, f, format='vtt'):
        if format == 'vtt':
            WebVTTWriter().write(self._captions, f)
        elif format == 'srt':
            SRTWriter().write(self._captions, f)
#        elif output_format == OutputFormat.SBV:
#            SBVWriter().write(self._captions, f)

    @staticmethod
    def list_formats():
        """Provides a list of supported formats that this class can read from."""
        return ('WebVTT (.vtt)', 'SubRip (.srt)', 'YouTube SBV (.sbv)')

    @property
    def captions(self):
        """Returns the list of captions."""
        return self._captions

    @property
    def total_length(self):
        """Returns the total length of the captions."""
        if not self._captions:
            return 0
        return int(self._captions[-1].end_in_seconds) - int(self._captions[0].start_in_seconds)

    @property
    def styles(self):
        return self._styles

    @property
    def content(self):
        """
        Return webvtt content with webvtt formatting.

        This property is useful in cases where the webvtt content is needed
        but no file saving on the system is required.
        """
        return WebVTTWriter().webvtt_content(self._captions)
