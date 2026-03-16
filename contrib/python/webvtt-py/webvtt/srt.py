from .main import Captions
from .parsers import SRTParser

class SRTCaptions(Captions):

    OUR_PARSER = SRTParser

    def save_format(self, output=''):
        """Save the document.
        If no output is provided the file will be saved in the same location. Otherwise output
        can determine a target directory or file.
        """
        with open(self.file, 'w', encoding='utf-8') as f:
            line_number = 1
            for c in self._captions:
                f.write('{}\n'.format(line_number))
                f.write('{} --> {}\n'.format(self._to_srt_timestamp(c.start_in_seconds), self._to_srt_timestamp(c.end_in_seconds)))
                f.writelines(['{}\n'.format(l) for l in c.lines])
                f.write('\n')
                line_number += 1

    def _to_srt_timestamp(self, total_seconds):
        hours = int(total_seconds / 3600)
        minutes = int(total_seconds / 60 - hours * 60)
        seconds = int(total_seconds - hours * 3600 - minutes * 60)
        milliseconds = round((total_seconds - seconds - hours * 3600 - minutes * 60)*1000)
        print(total_seconds)
        print('{:02d}:{:02d}:{:02d},{:03d}'.format(hours, minutes, seconds, milliseconds))
        return '{:02d}:{:02d}:{:02d},{:03d}'.format(hours, minutes, seconds, milliseconds)
