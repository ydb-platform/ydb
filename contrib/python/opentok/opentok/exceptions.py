class OpenTokException(Exception):
    """Defines exceptions thrown by the OpenTok SDK."""


class RequestError(OpenTokException):
    """Indicates an error during the request. Most likely an error connecting
    to the OpenTok API servers. (HTTP 500 error).
    """


class AuthError(OpenTokException):
    """Indicates that the problem was likely with credentials. Check your API
    key and API secret and try again.
    """


class NotFoundError(OpenTokException):
    """Indicates that the element requested was not found.  Check the parameters
    of the request.
    """


class ArchiveError(OpenTokException):
    """Indicates that there was a archive specific problem, probably the status
    of the requested archive is invalid.
    """


class SignalingError(OpenTokException):
    """Indicates that there was a signaling specific problem, one of the parameter
    is invalid or the type|data string doesn't have a correct size"""


class GetStreamError(OpenTokException):
    """Indicates that the data in the request is invalid, or the session_id or stream_id
    are invalid"""


class ForceDisconnectError(OpenTokException):
    """
    Indicates that there was a force disconnect specific problem:
    One of the arguments is invalid or the client specified by the connectionId property
    is not connected to the session
    """


class SipDialError(OpenTokException):
    """
    Indicates that there was a SIP dial specific problem:
    The Session ID passed in is invalid or you attempt to start a SIP call for a session
    that does not use the OpenTok Media Router.
    """


class SetStreamClassError(OpenTokException):
    """
    Indicates that there is invalid data in the JSON request.
    It may also indicate that invalid layout options have been passed.
    """


class BroadcastError(OpenTokException):
    """
    Indicates that data in your request data is invalid JSON. It may also indicate
    that you passed in invalid layout options. Or you have exceeded the limit of five
    simultaneous RTMP streams for an OpenTok session. Or you specified and invalid resolution.
    Or The broadcast has already started for the session
    """


class DTMFError(OpenTokException):
    """
    Indicates that one of the properties digits, session_id or connection_id is invalid
    """


class ArchiveStreamModeError(OpenTokException):
    """
    Indicates that the archive is configured with a streamMode that does not support stream manipulation.
    """


class BroadcastStreamModeError(OpenTokException):
    """
    Indicates that the broadcast is configured with a streamMode that does not support stream manipulation.
    """


class BroadcastHLSOptionsError(OpenTokException):
    """
    Indicates that HLS options have been set incorrectly.

    dvr and lowLatency modes cannot both be set to true in a broadcast.
    """


class BroadcastOptionsError(OpenTokException):
    """
    Indicates that broadcast options have been set incorrectly.
    """


class InvalidWebSocketOptionsError(OpenTokException):
    """
    Indicates that the WebSocket options selected are invalid.
    """


class InvalidMediaModeError(OpenTokException):
    """
    Indicates that the media mode selected was not valid for the type of request made.
    """


class CaptioningAlreadyInProgressError(OpenTokException):
    """
    Indicates that captioning was requested for an OpenTok session where live captions have already started.
    """
