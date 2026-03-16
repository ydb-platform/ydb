"""
Python wrapper for ffprobe command line tool. ffprobe must exist in the path.
"""
import functools
import operator
import os
import shlex
import platform
import re
import subprocess

from ffprobe.exceptions import FFProbeError


class FFProbe:
    """
    FFProbe wraps the ffprobe command and pulls the data into an object form::
        metadata=FFProbe('multimedia-file.mov')
    """

    def __init__(self, path_to_video):
        self.path_to_video = path_to_video

        try:
            with open(os.devnull, 'w') as tempf:
                subprocess.check_call(["ffprobe", "-h"], stdout=tempf, stderr=tempf)
        except FileNotFoundError:
            raise IOError('ffprobe not found.')

        if os.path.isfile(self.path_to_video):
            if platform.system() == 'Windows':
                cmd = ["ffprobe", "-show_streams", self.path_to_video]
            else:
                cmd = ["ffprobe -show_streams " + shlex.quote(self.path_to_video)]

            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

            stream = False
            self.streams = []
            self.video = []
            self.audio = []
            self.subtitle = []
            self.attachment = []

            for line in iter(p.stdout.readline, b''):
                line = line.decode('UTF-8')

                if '[STREAM]' in line:
                    stream = True
                    data_lines = []
                elif '[/STREAM]' in line and stream:
                    stream = False
                    # noinspection PyUnboundLocalVariable
                    self.streams.append(FFStream(data_lines))
                elif stream:
                    data_lines.append(line)

            self.metadata = {}
            is_metadata = False
            stream_metadata_met = False

            for line in iter(p.stderr.readline, b''):
                line = line.decode('UTF-8')

                if 'Metadata:' in line and not stream_metadata_met:
                    is_metadata = True
                elif 'Stream #' in line:
                    is_metadata = False
                    stream_metadata_met = True
                elif is_metadata:
                    splits = line.split(',')
                    for s in splits:
                        m = re.search(r'(\w+)\s*:\s*(.*)$', s)
                        # print(m.groups())
                        self.metadata[m.groups()[0]] = m.groups()[1].strip()

                if '[STREAM]' in line:
                    stream = True
                    data_lines = []
                elif '[/STREAM]' in line and stream:
                    stream = False
                    self.streams.append(FFStream(data_lines))
                elif stream:
                    data_lines.append(line)

            # print(self.metadata)

            p.stdout.close()
            p.stderr.close()

            for stream in self.streams:
                if stream.is_audio():
                    self.audio.append(stream)
                elif stream.is_video():
                    self.video.append(stream)
                elif stream.is_subtitle():
                    self.subtitle.append(stream)
                elif stream.is_attachment():
                    self.attachment.append(stream)
        else:
            raise IOError('No such media file ' + self.path_to_video)

    def __repr__(self):
        return "<FFprobe: {metadata}, {video}, {audio}, {subtitle}, {attachment}>".format(**vars(self))


class FFStream:
    """
    An object representation of an individual stream in a multimedia file.
    """

    def __init__(self, data_lines):
        for line in data_lines:
            self.__dict__.update({key: value for key, value, *_ in [line.strip().split('=')]})

            try:
                self.__dict__['framerate'] = round(
                    functools.reduce(
                        operator.truediv, map(int, self.__dict__.get('avg_frame_rate', '').split('/'))
                    )
                )

            except ValueError:
                self.__dict__['framerate'] = None
            except ZeroDivisionError:
                self.__dict__['framerate'] = 0

    def __repr__(self):
        if self.is_video():
            template = "<Stream: #{index} [{codec_type}] {codec_long_name}, {framerate}, ({width}x{height})>"

        elif self.is_audio():
            template = "<Stream: #{index} [{codec_type}] {codec_long_name}, channels: {channels} ({channel_layout}), " \
                       "{sample_rate}Hz> "

        elif self.is_subtitle() or self.is_attachment():
            template = "<Stream: #{index} [{codec_type}] {codec_long_name}>"

        else:
            template = ''

        return template.format(**self.__dict__)

    def is_audio(self):
        """
        Is this stream labelled as an audio stream?
        """
        return self.__dict__.get('codec_type', None) == 'audio'

    def is_video(self):
        """
        Is the stream labelled as a video stream.
        """
        return self.__dict__.get('codec_type', None) == 'video'

    def is_subtitle(self):
        """
        Is the stream labelled as a subtitle stream.
        """
        return self.__dict__.get('codec_type', None) == 'subtitle'

    def is_attachment(self):
        """
        Is the stream labelled as a attachment stream.
        """
        return self.__dict__.get('codec_type', None) == 'attachment'

    def frame_size(self):
        """
        Returns the pixel frame size as an integer tuple (width,height) if the stream is a video stream.
        Returns None if it is not a video stream.
        """
        size = None
        if self.is_video():
            width = self.__dict__['width']
            height = self.__dict__['height']

            if width and height:
                try:
                    size = (int(width), int(height))
                except ValueError:
                    raise FFProbeError("None integer size {}:{}".format(width, height))
        else:
            return None

        return size

    def pixel_format(self):
        """
        Returns a string representing the pixel format of the video stream. e.g. yuv420p.
        Returns none is it is not a video stream.
        """
        return self.__dict__.get('pix_fmt', None)

    def frames(self):
        """
        Returns the length of a video stream in frames. Returns 0 if not a video stream.
        """
        if self.is_video() or self.is_audio():
            try:
                frame_count = int(self.__dict__.get('nb_frames', ''))
            except ValueError:
                raise FFProbeError('None integer frame count')
        else:
            frame_count = 0

        return frame_count

    def duration_seconds(self):
        """
        Returns the runtime duration of the video stream as a floating point number of seconds.
        Returns 0.0 if not a video stream.
        """
        if self.is_video() or self.is_audio():
            try:
                duration = float(self.__dict__.get('duration', ''))
            except ValueError:
                raise FFProbeError('None numeric duration')
        else:
            duration = 0.0

        return duration

    def language(self):
        """
        Returns language tag of stream. e.g. eng
        """
        return self.__dict__.get('TAG:language', None)

    def codec(self):
        """
        Returns a string representation of the stream codec.
        """
        return self.__dict__.get('codec_name', None)

    def codec_description(self):
        """
        Returns a long representation of the stream codec.
        """
        return self.__dict__.get('codec_long_name', None)

    def codec_tag(self):
        """
        Returns a short representative tag of the stream codec.
        """
        return self.__dict__.get('codec_tag_string', None)

    def bit_rate(self):
        """
        Returns bit_rate as an integer in bps
        """
        try:
            return int(self.__dict__.get('bit_rate', ''))
        except ValueError:
            raise FFProbeError('None integer bit_rate')
