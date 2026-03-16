from datetime import datetime, date
from six import iteritems, PY2, PY3, u
import json
import pytz
from enum import Enum

from .exceptions import ArchiveError

if PY3:
    from datetime import timezone

# compat
from six.moves import map

dthandler = lambda obj: (
    obj.isoformat() if isinstance(obj, datetime) or isinstance(obj, date) else None
)


class OutputModes(Enum):
    """List of valid settings for the output_mode parameter of the OpenTok.start_archive()
    method."""

    composed = u("composed")
    """All streams in the archive are recorded to a single (composed) file."""
    individual = u("individual")
    """Each stream in the archive is recorded to an individual file."""


class StreamModes(Enum):
    """ "List of valid settings for the stream_mode parameter of the OpenTok.start_archive()
    method."""

    auto = u("auto")
    """Streams are automatically added to the archive."""
    manual = u("manual")
    """Streams are included in the archive based on calls to the OpenTok.add_archive_stream()
    and OpenTok.remove_archive_stream() methods."""


class Archive(object):
    """Represents an archive of an OpenTok session.

    :ivar created_at:
       The time at which the archive was created, in milliseconds since the UNIX epoch.

    :ivar duration:
       The duration of the archive, in seconds.

    :ivar has_audio:
       Boolean value set to true when the archive contains an audio track,
       and set to false otherwise.

    :ivar has_video:
       Boolean value set to true when the archive contains a video track,
       and set to false otherwise.

    :ivar id:
       The archive ID.

    :ivar name:
       The name of the archive. If no name was provided when the archive was created, this is set
       to null.

    :ivar output_mode:
        Whether all streams in the archive are recorded to a single file
        (OutputModes.composed) or to individual files (OutputModes.individual).

    :ivar streamMode:
        Whether streams included in the archive are selected automatically
        ("auto", the default) or manually ("manual").

    :ivar streams:
        A list of streams currently being archived. This is only set for an archive with
        the status set to "started" and the stream_Mode set to "manual".

    :ivar partner_id:
       The API key associated with the archive.

    :ivar reason:
       For archives with the status "stopped" or "failed", this string describes the
       reason the archive stopped (such as "maximum duration exceeded") or failed.

    :ivar session_id:
       The session ID of the OpenTok session associated with this archive.

    :ivar size:
       The size of the MP4 file. For archives that have not been generated, this value is set to 0.

    :ivar status:
       The status of the archive, which can be one of the following:

       * "available" -- The archive is available for download from the OpenTok cloud.
       * "expired" -- The archive is no longer available for download from the OpenTok cloud.
       * "failed" -- The archive recording failed.
       * "paused" -- The archive is in progress and no clients are publishing streams to the
         session. When an archive is in progress and any client publishes a stream, the status is
         "started". When an archive is paused, nothing is recorded. When a client starts publishing
         a stream, the recording starts (or resumes). If all clients disconnect from a session that
         is being archived, the status changes to "paused", and after 60 seconds the archive
         recording stops (and the status changes to "stopped").
       * "started" -- The archive started and is in the process of being recorded.
       * "stopped" -- The archive stopped recording.
       * "uploaded" -- The archive is available for download from the the upload target
         Amazon S3 bucket or Windows Azure container that you set at the
         `OpenTok dashboard <https://dashboard.tokbox.com>`_.

    :ivar url:
       The download URL of the available MP4 file. This is only set for an archive with the status set to
       "available"; for other archives, (including archives with the status "uploaded") this property is
       set to null. The download URL is obfuscated, and the file is only available from the URL for
       10 minutes. To generate a new URL, call the Archive.listArchives() or OpenTok.getArchive() method.

    :ivar max_bitrate: The maximum video bitrate for the archive, in bits per second. The minimum value is 100,000 and the maximum is 6,000,000.
    
    :ivar quantization_parameter: The quantization parameter (QP) for video encoding quality. Values between 15-40, where smaller values generate higher quality and larger archives.
    """

    def __init__(self, sdk, values):
        self.sdk = sdk
        self.id = values.get("id")
        self.name = values.get("name")
        self.status = values.get("status")
        self.session_id = values.get("sessionId")
        self.partner_id = values.get("partnerId")
        if PY2:
            self.created_at = datetime.fromtimestamp(
                values.get("createdAt") / 1000, pytz.UTC
            )
        if PY3:
            self.created_at = datetime.fromtimestamp(
                values.get("createdAt") // 1000, timezone.utc
            )
        self.size = values.get("size")
        self.duration = values.get("duration")
        self.has_audio = values.get("hasAudio")
        self.has_video = values.get("hasVideo")
        self.output_mode = OutputModes[values.get("outputMode", "composed")]
        self.stream_mode = values.get("streamMode", StreamModes.auto)
        self.streams = values.get("streams")
        self.url = values.get("url")
        self.resolution = values.get("resolution")
        self.max_bitrate = values.get("maxBitrate")
        self.quantization_parameter = values.get("quantizationParameter")

    def stop(self):
        """
        Stops an OpenTok archive that is being recorded.

        Archives automatically stop recording after 120 minutes or when all clients have
        disconnected from the session being archived.
        """
        temp_archive = self.sdk.stop_archive(self.id)
        for k, v in iteritems(temp_archive.attrs()):
            setattr(self, k, v)

    def delete(self):
        """
        Deletes an OpenTok archive.

        You can only delete an archive which has a status of "available" or "uploaded". Deleting an
        archive removes its record from the list of archives. For an "available" archive, it also
        removes the archive file, making it unavailable for download.
        """
        self.sdk.delete_archive(self.id)
        # TODO: invalidate this object

    def attrs(self):
        """
        Returns a dictionary of the archive's attributes.
        """
        return dict((k, v) for k, v in iteritems(self.__dict__) if k != "sdk")

    def json(self):
        """
        Returns a JSON representation of the archive.
        """
        return json.dumps(self.attrs(), default=dthandler, indent=4)


class ArchiveList(object):
    def __init__(self, sdk, values):
        self.count = values.get("count")
        self.items = list(map(lambda x: Archive(sdk, x), values.get("items", [])))

    def __iter__(self):
        for x in self.items:
            yield x

    def attrs(self):
        return {"count": self.count, "items": map(Archive.attrs, self.items)}

    def json(self):
        return json.dumps(self.attrs(), default=dthandler, indent=4)

    def __getitem__(self, key):
        return self.items.get(key)

    def __setitem__(self, key, item):
        raise ArchiveError(
            u("Cannot set item {0} for key {1} in Archive object").format(item, key)
        )

    def __len__(self):
        return len(self.items)
