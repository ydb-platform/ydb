import json
from enum import Enum
from six import u


class Broadcast(object):
    """
    Represents a live streaming broadcast.

    :ivar id:
        The broadcast ID.

    :ivar session_id:
       The session ID of the OpenTok session associated with this broadcast.

    :ivar project_id:
        Your OpenTok API key.

    :ivar created_at:
       The time at which the broadcast was created, in milliseconds since the UNIX epoch.

    :ivar updated_at:
       The time at which the broadcast was last updated, in milliseconds since the UNIX epoch.

    :ivar resolution:
        The resolution of the broadcast (either "640x480", "1280x720", "1920x1080", "480x640", "720x1280", or "1920x1080").

    :ivar status:
        The status of the broadcast.

    :ivar hasAudio:
        Whether the broadcast has audio.

    :ivar hasVideo:
        Whether the broadcast has video.

    :ivar 'maxBitrate' optional:
        The maximum bitrate (bits per second) used by the broadcast.

    :ivar broadcastUrls:
        An object containing details about the HLS and RTMP broadcasts.

        If you specified an HLS endpoint, the object includes an hls property, which is set to the URL for the HLS broadcast.
        Note this HLS broadcast URL points to an index file, an .M3U8-formatted playlist that contains a list of URLs
        to .ts media segment files (MPEG-2 transport stream files).
        While the URLs of both the playlist index file and media segment files are provided as soon as the HTTP response
        is returned, these URLs should not be accessed until 15 - 20 seconds later,
        after the initiation of the HLS broadcast, due to the delay between the HLS broadcast and the live streams
        in the OpenTok session.
        See https://developer.apple.com/library/ios/technotes/tn2288/_index.html for more information about the playlist index
        file and media segment files for HLS.

        If you specified an HLS endpoint, the object will also include an "hlsStatus" property with
        information about the HLS broadcast. This will have one of the following values:
            ["connecting", "ready", "live", "ended", "error"].

        If you specified RTMP stream endpoints, the object includes an rtmp property.
        This is an array of objects that include information on each of the RTMP streams.
        Each of these objects has the following properties: id (the ID you assigned to the RTMP stream),
        serverUrl (the server URL), streamName (the stream name), and status property (which is set to "connecting").
        You can call the OpenTok REST method to check for status updates for the broadcast:
        https://tokbox.com/developer/rest/#get_info_broadcast

    :ivar streamMode:
        Whether streams included in the broadcast are selected automatically
        ("auto", the default) or manually ("manual").

    :ivar streams:
        A list of streams currently being broadcasted. This is only set for a broadcast with
        the status set to "started" and the stream_Mode set to "manual".
    """

    def __init__(self, kwargs):
        self.id = kwargs.get("id")
        self.sessionId = kwargs.get("sessionId")
        self.projectId = kwargs.get("projectId")
        self.createdAt = kwargs.get("createdAt")
        self.updatedAt = kwargs.get("updatedAt")
        self.hasAudio = kwargs.get("hasAudio")
        self.hasVideo = kwargs.get("hasVideo")
        self.maxBitrate = kwargs.get("maxBitrate")
        self.maxDuration = kwargs.get("maxDuration")
        self.resolution = kwargs.get("resolution")
        self.status = kwargs.get("status")
        self.broadcastUrls = kwargs.get("broadcastUrls")
        self.stream_mode = kwargs.get("streamMode", BroadcastStreamModes.auto)
        self.streams = kwargs.get("streams")

    def json(self):
        """
        Returns a JSON representation of the broadcast.
        """
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BroadcastStreamModes(Enum):
    """ "List of valid settings for the stream_mode parameter of the OpenTok.start_broadcast()
    method."""

    auto = u("auto")
    """Streams are automatically added to the broadcast."""
    manual = u("manual")
    """Streams are included in the broadcast based on calls to the OpenTok.add_broadcast_stream()
    and OpenTok.remove_broadcast_stream() methods."""
