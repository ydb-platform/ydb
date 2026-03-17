import warnings


class Endpoints(object):
    """
    For internal use.
    Class that provides the endpoint urls
    """

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    def get_session_url(self):
        url = self.api_url + "/session/create"
        return url

    def session_url(self):
        warnings.warn(
            "endpoints.session_url is deprecated (use endpoints.get_session_url instead).",
            DeprecationWarning,
            stacklevel=2,
        )

        return self.get_session_url()

    def get_archive_url(self, archive_id=None):
        url = self.api_url + "/v2/project/" + self.api_key + "/archive"
        if archive_id:
            url = url + "/" + archive_id
        return url

    def archive_url(self, archive_id=None):
        warnings.warn(
            "endpoints.archive_url is deprecated (use endpoints.get_archive_url instead).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_archive_url(archive_id)

    def get_signaling_url(self, session_id, connection_id=None):
        url = self.api_url + "/v2/project/" + self.api_key + "/session/" + session_id

        if connection_id:
            url += "/connection/" + connection_id

        url += "/signal"
        return url

    def signaling_url(self, session_id, connection_id=None):
        warnings.warn(
            "endpoints.signaling_url is deprecated (use endpoints.get_signaling_url instead).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_signaling_url(session_id, connection_id)

    def get_stream_url(self, session_id, stream_id=None):
        """this method returns the url to get streams information"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/session/"
            + session_id
            + "/stream"
        )
        if stream_id:
            url = url + "/" + stream_id
        return url

    def broadcast_url(self, broadcast_id=None, stop=False, layout=False):
        warnings.warn(
            "endpoints.broadcast_url is deprecated (use endpoints.get_broadcast_url instead).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_broadcast_url(broadcast_id, stop, layout)

    def force_disconnect_url(self, session_id, connection_id):
        """this method returns the force disconnect url endpoint"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/session/"
            + session_id
            + "/connection/"
            + connection_id
        )
        return url

    def set_archive_layout_url(self, archive_id):
        """this method returns the url to set the archive layout"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/archive/"
            + archive_id
            + "/layout"
        )
        return url

    def dial_url(self):
        """this method returns the url to initialize a SIP call"""
        url = self.api_url + "/v2/project/" + self.api_key + "/dial"
        return url

    def set_stream_class_lists_url(self, session_id):
        """this method returns the url to set the stream class list"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/session/"
            + session_id
            + "/stream"
        )
        return url

    def get_broadcast_url(self, broadcast_id=None, stop=False, layout=False):
        """this method returns urls for working with broadcast"""
        url = self.api_url + "/v2/project/" + self.api_key + "/broadcast"

        if broadcast_id:
            url = url + "/" + broadcast_id
        if stop:
            url = url + "/stop"
        if layout:
            url = url + "/layout"

        return url

    def get_mute_all_url(self, session_id):
        """this method returns the urls for muting every stream in a session"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/session/"
            + session_id
            + "/mute"
        )

        return url

    def get_dtmf_all_url(self, session_id):
        """this method returns the url for Play DTMF to all clients in the session"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/session/"
            + session_id
            + "/play-dtmf"
        )

        return url

    def get_dtmf_specific_url(self, session_id, connection_id):
        """this method returns the url for Play DTMF to a specific client connection"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/session/"
            + session_id
            + "/connection/"
            + connection_id
            + "/play-dtmf"
        )

        return url

    def get_archive_stream(self, archive_id=None):
        """this method returns the url for working with streamModes in archives"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/archive/"
            + archive_id
            + "/streams"
        )

        return url

    def get_broadcast_stream(self, broadcast_id=None):
        """this method returns the url for working with streamModes in broadcasts"""
        url = (
            self.api_url
            + "/v2/project/"
            + self.api_key
            + "/broadcast/"
            + broadcast_id
            + "/streams"
        )

        return url

    def get_render_url(self, render_id: str = None):
        "Returns the URL for working with the Render API." ""
        url = self.api_url + "/v2/project/" + self.api_key + "/render"
        if render_id:
            url += "/" + render_id

        return url

    def get_audio_connector_url(self):
        """Returns the  URL for working with the Audio Connector API."""
        url = self.api_url + "/v2/project/" + self.api_key + "/connect"

        return url

    def get_captions_url(self, captions_id: str = None):
        """Returns the URL for working with the Captions API."""
        url = self.api_url + '/v2/project/' + self.api_key + '/captions'
        if captions_id:
            url += f'/{captions_id}/stop'

        return url
