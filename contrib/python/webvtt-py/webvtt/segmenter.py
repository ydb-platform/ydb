import os
from math import ceil, floor

from .errors import InvalidCaptionsError
from .webvtt import WebVTT
from .structures import Caption

MPEGTS = 900000
SECONDS = 10  # default number of seconds per segment

__all__ = ['WebVTTSegmenter']


class WebVTTSegmenter(object):
    """
    Provides segmentation of WebVTT captions for HTTP Live Streaming (HLS).
    """
    def __init__(self):
        self._total_segments = 0
        self._output_folder = ''
        self._seconds = 0
        self._mpegts = 0
        self._segments = []

    def _validate_webvtt(self, webvtt):
        # Validates that the captions is a list and all the captions are instances of Caption.
        if not isinstance(webvtt, WebVTT):
            return False
        for c in webvtt.captions:
            if not isinstance(c, Caption):
                return False
        return True

    def _slice_segments(self, captions):
        self._segments = [[] for _ in range(self.total_segments)]

        for c in captions:
            segment_index_start = floor(c.start_in_seconds / self.seconds)
            self.segments[segment_index_start].append(c)

            # Also include a caption in other segments based on the end time.
            segment_index_end = floor(c.end_in_seconds / self.seconds)
            if segment_index_end > segment_index_start:
                for i in range(segment_index_start + 1, segment_index_end + 1):
                    self.segments[i].append(c)

    def _write_segments(self):
        for index in range(self.total_segments):
            segment_file = os.path.join(self._output_folder, 'fileSequence{}.webvtt'.format(index))

            with open(segment_file, 'w', encoding='utf-8') as f:
                f.write('WEBVTT\n')
                f.write('X-TIMESTAMP-MAP=MPEGTS:{},LOCAL:00:00:00.000\n'.format(self._mpegts))

                for caption in self.segments[index]:
                    f.write('\n{} --> {}\n'.format(caption.start, caption.end))
                    f.writelines(['{}\n'.format(l) for l in caption.lines])

    def _write_manifest(self):
        manifest_file = os.path.join(self._output_folder, 'prog_index.m3u8')
        with open(manifest_file, 'w', encoding='utf-8') as f:
            f.write('#EXTM3U\n')
            f.write('#EXT-X-TARGETDURATION:{}\n'.format(self.seconds))
            f.write('#EXT-X-VERSION:3\n')
            f.write('#EXT-X-PLAYLIST-TYPE:VOD\n')

            for i in range(self.total_segments):
                f.write('#EXTINF:30.00000\n')
                f.write('fileSequence{}.webvtt\n'.format(i))

            f.write('#EXT-X-ENDLIST\n')

    def segment(self, webvtt, output='', seconds=SECONDS, mpegts=MPEGTS):
        """Segments the captions based on a number of seconds."""
        if isinstance(webvtt, str):
            # if a string is supplied we parse the file
            captions = WebVTT().read(webvtt).captions
        elif not self._validate_webvtt(webvtt):
            raise InvalidCaptionsError('The captions provided are invalid')
        else:
            # we expect to have a webvtt object
            captions = webvtt.captions

        self._total_segments = 0 if not captions else int(ceil(captions[-1].end_in_seconds / seconds))
        self._output_folder = output
        self._seconds = seconds
        self._mpegts = mpegts

        output_folder = os.path.join(os.getcwd(), output)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        self._slice_segments(captions)
        self._write_segments()
        self._write_manifest()

    @property
    def seconds(self):
        """Returns the number of seconds used for segmenting captions."""
        return self._seconds

    @property
    def total_segments(self):
        """Returns the total of segments."""
        return self._total_segments

    @property
    def segments(self):
        """Return the list of segments."""
        return self._segments
