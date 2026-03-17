from datetime import datetime  # generate_token
import re
from typing import List, Optional  # imports List, Optional type hint
import calendar  # generate_token
import base64  # generate_token
import random  # generate_token
import time  # generate_token
import hmac  # _sign_string
import hashlib
from typing import List
import uuid
import requests  # create_session, archiving
import json  # archiving
import platform  # user-agent
from socket import inet_aton  # create_session
import xml.dom.minidom as xmldom  # create_session
from jwt import encode  # _create_jwt_auth_header
import random  # _create_jwt_auth_header
import logging  # logging
import warnings  # Native. Used for notifying deprecations


# compat
from urllib.parse import urlencode
from six import text_type, u, b, PY3
from enum import Enum

from .version import __version__
from .endpoints import Endpoints
from .session import Session
from .archives import Archive, ArchiveList, OutputModes, StreamModes
from .captions import Captions
from .render import Render, RenderList
from .stream import Stream
from .streamlist import StreamList
from .sip_call import SipCall
from .broadcast import Broadcast, BroadcastStreamModes
from .websocket_audio_connection import WebSocketAudioConnection
from .exceptions import (
    ArchiveStreamModeError,
    BroadcastOptionsError,
    BroadcastHLSOptionsError,
    BroadcastStreamModeError,
    OpenTokException,
    RequestError,
    AuthError,
    NotFoundError,
    ArchiveError,
    SignalingError,
    GetStreamError,
    ForceDisconnectError,
    SipDialError,
    SetStreamClassError,
    BroadcastError,
    DTMFError,
    InvalidWebSocketOptionsError,
    InvalidMediaModeError,
    CaptioningAlreadyInProgressError,
)


class Roles(Enum):
    """List of valid roles for a token."""

    subscriber = u("subscriber")
    """A subscriber can only subscribe to streams."""
    publisher = u("publisher")
    """A publisher can publish streams, subscribe to streams, and signal"""
    publisher_only = "publisheronly"
    """A client with the `publisher_only` role can only publish streams. It cannot subscribe to 
    other clients' streams or send signals."""
    moderator = u("moderator")
    """In addition to the privileges granted to a publisher, a moderator can perform
    moderation functions, such as forcing clients to disconnect, to stop publishing streams,
    or to mute audio in published streams. See the
    `Moderation developer guide <https://tokbox.com/developer/guides/moderation/>`_
    """


class MediaModes(Enum):
    """List of valid settings for the mediaMode parameter of the OpenTok.create_session() method."""

    routed = u("disabled")
    """The session will transmit streams using the OpenTok Media Server."""
    relayed = u("enabled")
    """The session will attempt to transmit streams directly between clients. If two clients
    cannot send and receive each others' streams, due to firewalls on the clients' networks,
    their streams will be relayed using the OpenTok TURN Server."""


class ArchiveModes(Enum):
    """List of valid settings for the archive_mode parameter of the OpenTok.create_Session()
    method."""

    manual = u("manual")
    """The session will be manually archived."""
    always = u("always")
    """The session will be automatically archived."""


valid_archive_resolutions = {
    "640x480",
    "480x640",
    "1280x720",
    "720x1280",
    "1920x1080",
    "1080x1920",
}

logger = logging.getLogger("opentok")


class Client(object):
    """Use this SDK to create tokens and interface with the server-side portion
    of the Opentok API.

    You can also interact with this client object with Vonage credentials. Instead of passing
    on OpenTok API key and secret, you can pass in a Vonage application ID and private key,
    e.g. api_key=VONAGE_APPLICATION_ID, api_secret=VONAGE_PRIVATE_KEY. You do not need to set the API
    URL differently, the SDK will set this for you.
    """

    TOKEN_SENTINEL = "T1=="
    """For internal use."""

    def __init__(
        self,
        api_key,
        api_secret,
        api_url="https://api.opentok.com",
        timeout=None,
        app_version=None,
    ):

        if isinstance(api_secret, (str, bytes)) and re.search(
            "[.][a-zA-Z0-9_]+$", api_secret
        ):
            # We have a private key so we assume we are using Vonage credentials
            self._using_vonage = True
            self._api_url = 'https://video.api.vonage.com'
            with open(api_secret, "rb") as key_file:
                self.api_secret = key_file.read()
        else:
            # We are using OpenTok credentials
            self._using_vonage = False
            self.api_secret = api_secret
            self._api_url = api_url

        self.api_key = str(api_key)
        self.timeout = timeout
        self._proxies = None
        self.endpoints = Endpoints(self._api_url, self.api_key)
        self._app_version = __version__ if app_version == None else app_version
        self._user_agent = (
            f"OpenTok-Python-SDK/{self.app_version} python/{platform.python_version()}"
        )
        # JWT custom claims - Default values
        self._jwt_livetime = 3  # In minutes

    @property
    def proxies(self):
        return self._proxies

    @proxies.setter
    def proxies(self, proxies):
        self._proxies = proxies

    @property
    def app_version(self):
        return self._app_version

    @app_version.setter
    def app_version(self, value):
        self._app_version = value

    @property
    def user_agent(self):
        return self._user_agent

    def append_to_user_agent(self, value):
        self._user_agent = self._user_agent + value

    @property
    def jwt_livetime(self):
        return self._jwt_livetime

    @jwt_livetime.setter
    def jwt_livetime(self, minutes):
        self._jwt_livetime = minutes

    def generate_token(
        self,
        session_id,
        role=Roles.publisher,
        expire_time=None,
        data=None,
        initial_layout_class_list=[],
        use_jwt=True,
    ):
        """
        Generates a token for a given session.

        :param String session_id: The session ID of the session to be accessed by the client using
          the token.

        :param String role: The role for the token. Valid values are defined in the Role
          class:

          * `Roles.subscriber` -- A subscriber can only subscribe to streams.

          * `Roles.publisher` -- A publisher can publish streams, subscribe to
            streams, and signal. (This is the default value if you do not specify a role.)

          * `Roles.publisher_only` -- A client with the `publisher_only` role can only publish streams.
            It cannot subscribe to other clients' streams or send signals.

          * `Roles.moderator` -- In addition to the privileges granted to a
            publisher, in clients using the OpenTok.js 2.2 library, a moderator can call the
            `forceUnpublish()` and `forceDisconnect()` method of the
            Session object.

        :param int expire_time: The expiration time of the token, in seconds since the UNIX epoch.
          The maximum expiration time is 30 days after the creation time. The default expiration
          time is 24 hours after the token creation time.

        :param String data: A string containing connection metadata describing the
          end-user. For example, you can pass the user ID, name, or other data describing the
          end-user. The length of the string is limited to 1000 characters. This data cannot be
          updated once it is set.

        :param list initial_layout_class_list: An array of class names (strings)
          to be used as the initial layout classes for streams published by the client. Layout
          classes are used in customizing the layout of videos in
          `live streaming broadcasts <https://tokbox.com/developer/guides/broadcast/#live-streaming>`_ and
          `composed archives <https://tokbox.com/developer/guides/archiving/layout-control.html>`_

        :param bool use_jwt: Whether to use JWT tokens or not. If set to False, the token will be a
            plain text token. If set to True (the default), the token will be a JWT.

        :rtype:
          The token string.
        """

        # normalize
        # expire_time can be an integer, a datetime object, or anything else that can be coerced into an integer
        # after this block it will only be an integer
        if expire_time is not None:
            if isinstance(expire_time, datetime):
                expire_time = calendar.timegm(expire_time.utctimetuple())
            else:
                try:
                    expire_time = int(expire_time)
                except (ValueError, TypeError):
                    raise OpenTokException(
                        u("Cannot generate token, invalid expire time {0}").format(
                            expire_time
                        )
                    )
        else:
            expire_time = int(time.time()) + (60 * 60 * 24)  # 1 day

        # validations
        if not text_type(session_id):
            raise OpenTokException(
                u("Cannot generate token, session_id was not valid {0}").format(
                    session_id
                )
            )
        if not isinstance(role, Roles):
            raise OpenTokException(
                u("Cannot generate token, {0} is not a valid role").format(role)
            )
        now = int(time.time())
        if expire_time < now:
            raise OpenTokException(
                u("Cannot generate token, expire_time is not in the future {0}").format(
                    expire_time
                )
            )
        if expire_time > now + (60 * 60 * 24 * 30):  # 30 days
            raise OpenTokException(
                u(
                    "Cannot generate token, expire_time is not in the next 30 days {0}"
                ).format(expire_time)
            )
        if data and len(data) > 1000:
            raise OpenTokException(
                u("Cannot generate token, data must be less than 1000 characters")
            )
        if initial_layout_class_list and not all(
            text_type(c) for c in initial_layout_class_list
        ):
            raise OpenTokException(
                u(
                    "Cannot generate token, all items in initial_layout_class_list must be strings"
                )
            )
        initial_layout_class_list_serialized = u(" ").join(initial_layout_class_list)
        if len(initial_layout_class_list_serialized) > 1000:
            raise OpenTokException(
                u(
                    "Cannot generate token, initial_layout_class_list must be less than 1000 characters"
                )
            )

        # decode session id to verify api_key
        sub_session_id = session_id[2:]
        sub_session_id_bytes = sub_session_id.encode("utf-8")
        sub_session_id_bytes_padded = sub_session_id_bytes + (
            b("=") * (-len(sub_session_id_bytes) % 4)
        )
        try:
            decoded_session_id = base64.b64decode(sub_session_id_bytes_padded, b("-_"))
            parts = decoded_session_id.decode("utf-8").split(u("~"))
        except Exception:
            raise OpenTokException(
                u("Cannot generate token, the session_id {0} was not valid").format(
                    session_id
                )
            )
        if self.api_key not in parts:
            raise OpenTokException(
                u(
                    "Cannot generate token, the session_id {0} does not belong to the api_key {1}"
                ).format(session_id, self.api_key)
            )

        if use_jwt:
            payload = {}

            payload['session_id'] = session_id
            payload['role'] = role.value
            payload['iat'] = now
            payload["exp"] = expire_time
            payload['scope'] = 'session.connect'

            if initial_layout_class_list:
                payload['initial_layout_class_list'] = (
                    initial_layout_class_list_serialized
                )
            if data:
                payload['connection_data'] = data

            if not self._using_vonage:
                payload['iss'] = self.api_key
                payload['ist'] = 'project'
                payload['nonce'] = random.randint(0, 999999)

                headers = {'alg': 'HS256', 'typ': 'JWT'}

                token = encode(
                    payload, self.api_secret, algorithm="HS256", headers=headers
                )
            else:
                payload['application_id'] = self.api_key
                payload['jti'] = str(uuid.uuid4())
                payload['subject'] = 'video'
                payload['acl'] = {'paths': {'/session/**': {}}}

                headers = {'alg': 'RS256', 'typ': 'JWT'}

                token = encode(
                    payload, self.api_secret, algorithm="RS256", headers=headers
                )

            return token

        data_params = dict(
            session_id=session_id,
            create_time=now,
            expire_time=expire_time,
            role=role.value,
            nonce=random.randint(0, 999999),
            initial_layout_class_list=initial_layout_class_list_serialized,
        )
        if data:
            data_params["connection_data"] = data
        data_string = urlencode(data_params, True)

        sig = self._sign_string(data_string, self.api_secret)
        decoded_base64_bytes = u("partner_id={api_key}&sig={sig}:{payload}").format(
            api_key=self.api_key, sig=sig, payload=data_string
        )
        if PY3:
            decoded_base64_bytes = decoded_base64_bytes.encode("utf-8")
        token = u("{sentinal}{base64_data}").format(
            sentinal=self.TOKEN_SENTINEL,
            base64_data=base64.b64encode(decoded_base64_bytes).decode(),
        )

        return token

    def create_session(
        self,
        location=None,
        media_mode=MediaModes.relayed,
        archive_mode=ArchiveModes.manual,
        archive_name=None,
        archive_resolution=None,
        e2ee=False,
    ):
        """
        Creates a new OpenTok session and returns the session ID, which uniquely identifies
        the session.

        For example, when using the OpenTok JavaScript library, use the session ID when calling the
        OT.initSession() method (to initialize an OpenTok session).

        OpenTok sessions do not expire. However, authentication tokens do expire (see the
        generateToken() method). Also note that sessions cannot explicitly be destroyed.

        A session ID string can be up to 255 characters long.

        Calling this method results in an OpenTokException in the event of an error.
        Check the error message for details.

        You can also create a session using the OpenTok
        `REST API <https://tokbox.com/opentok/api/#session_id_production>`_ or
        `the OpenTok dashboard <https://dashboard.tokbox.com/projects>`_.

        :param String media_mode: Determines whether the session will transmit streams using the
             OpenTok Media Router (MediaMode.routed) or not (MediaMode.relayed). By default,
             the setting is MediaMode.relayed.

             With the media_mode property set to MediaMode.relayed, the session
             will attempt to transmit streams directly between clients. If clients cannot connect
             due to firewall restrictions, the session uses the OpenTok TURN server to relay
             audio-video streams.

             The `OpenTok Media
             Router <https://tokbox.com/opentok/tutorials/create-session/#media-mode>`_
             provides the following benefits:

               * The OpenTok Media Router can decrease bandwidth usage in multiparty sessions.
                   (When the mediaMode property is set to  MediaMode.relayed, each client must send
                   a separate audio-video stream to each client subscribing to it.)

               * The OpenTok Media Router can improve the quality of the user experience through
                 audio fallback and video recovery (see https://tokbox.com/platform/fallback). With
                 these features, if a client's connectivity degrades to a degree that
                 it does not support video for a stream it's subscribing to, the video is dropped on
                 that client (without affecting other clients), and the client receives audio only.
                 If the client's connectivity improves, the video returns.

               * The OpenTok Media Router supports the archiving feature, which lets
                 you record, save, and retrieve OpenTok sessions (see http://tokbox.com/platform/archiving).

        :param String archive_mode: Whether the session is automatically archived
            (ArchiveModes.always) or not (ArchiveModes.manual). By default,
            the setting is ArchiveModes.manual, and you must call the
            start_archive() method of the OpenTok object to start archiving. To archive the session
            (either automatically or not), you must set the media_mode parameter to
            MediaModes.routed.

        :param String archive_name: Indicates the archive name for all the archives in auto archived session.
            A session that begins with archive mode 'always' will be using this archive name for all archives of that session.
            Passing 'archive_name' with archive mode 'manual' will cause an error response.

        :param String archive_resolution:
            Indicates the archive resolution for all the archives in auto archived session.
            Valid values are '640x480', '480x640', '1280x720', '720x1280', '1920x1080' and '1080x1920'.
            A session that begins with archive mode 'always' will be using this resolution for all archives of that session.
            Passing 'archive_resolution' with archive mode 'manual' will cause an error response.

        :param String location: An IP address that the OpenTok servers will use to
            situate the session in its global network. If you do not set a location hint,
            the OpenTok servers will be based on the first client connecting to the session.

         :param Boolean e2ee: Whether to enable end-to-end encryption for a routed session
             (see https://tokbox.com/developer/guides/end-to-end-encryption/).

        :rtype: The Session object. The session_id property of the object is the session ID.
        """

        # build options
        options = {}
        if not isinstance(media_mode, MediaModes):
            raise OpenTokException(
                u("Cannot create session, {0} is not a valid media mode").format(
                    media_mode
                )
            )
        if not isinstance(archive_mode, ArchiveModes):
            raise OpenTokException(
                u("Cannot create session, {0} is not a valid archive mode").format(
                    archive_mode
                )
            )
        if archive_mode == ArchiveModes.always and media_mode != MediaModes.routed:
            raise OpenTokException(
                u(
                    "A session with always archive mode must also have the routed media mode."
                )
            )

        if archive_name is not None:
            if archive_mode == ArchiveModes.manual:
                raise OpenTokException(
                    "You cannot specify a value for archive_name with archive mode: manual."
                )
            if not 1 <= len(archive_name) <= 80:
                raise OpenTokException(
                    "archive_name must be between 1 and 80 characters in length."
                )

        if archive_resolution is not None:
            if archive_mode == ArchiveModes.manual:
                raise OpenTokException(
                    "You cannot specify a value for archive_resolution with archive mode: manual."
                )
            if archive_resolution not in valid_archive_resolutions:
                raise OpenTokException(
                    f"archive_resolution must be one of the allowed values: {valid_archive_resolutions}."
                )

        options[u("p2p.preference")] = media_mode.value
        options[u("archiveMode")] = archive_mode.value
        if archive_name is not None:
            options[("archiveName")] = archive_name
        if archive_resolution is not None:
            options[("archiveResolution")] = archive_resolution

        if location:
            # validate IP address
            try:
                inet_aton(location)
            except:
                raise OpenTokException(
                    u(
                        "Cannot create session. Location must be either None or a valid IPv4 address {0}"
                    ).format(location)
                )
            options[u("location")] = location
        options["e2ee"] = str(e2ee).lower()

        try:
            logger.debug(
                "POST to %r with params %r, headers %r, proxies %r",
                self.endpoints.get_session_url(),
                options,
                self.get_json_headers(),
                self.proxies,
            )
            if not self._using_vonage:
                response = requests.post(
                    self.endpoints.get_session_url(),
                    data=options,
                    headers=self.get_headers(),
                    proxies=self.proxies,
                    timeout=self.timeout,
                )
            else:
                headers = self.get_headers()
                response = requests.post(
                    self.endpoints.get_session_url(),
                    data=options,
                    headers=headers,
                    proxies=self.proxies,
                    timeout=self.timeout,
                )
            response.encoding = "utf-8"
            if response.status_code == 403:
                raise AuthError("Failed to create session, invalid credentials")
            if not response.content:
                raise RequestError()
        except Exception as e:
            raise RequestError("Failed to create session: %s" % str(e))

        try:
            content_type = response.headers["Content-Type"]
            # Legacy behaviour
            if content_type != "application/json":
                dom = xmldom.parseString(response.content.decode("utf-8"))
                error = dom.getElementsByTagName("error")
                if error:
                    error = error[0]
                    raise AuthError(
                        "Failed to create session (code=%s): %s"
                        % (
                            error.attributes["code"].value,
                            error.firstChild.attributes["message"].value,
                        )
                    )
                session_id = (
                    dom.getElementsByTagName("session_id")[0].childNodes[0].nodeValue
                )
            else:
                session_id = response.json()[0]["session_id"]
            return Session(
                self,
                session_id,
                location=location,
                media_mode=media_mode,
                archive_mode=archive_mode,
                e2ee=e2ee,
            )
        except Exception as e:
            raise OpenTokException("Failed to generate session: %s" % str(e))

    def get_headers(self):
        """For internal use."""
        if not self._using_vonage:
            return {
                "User-Agent": "OpenTok-Python-SDK/"
                + self.app_version
                + " python/"
                + platform.python_version(),
                "X-OPENTOK-AUTH": self._create_jwt_auth_header(),
                "Accept": "application/json",
            }
        return {
            "User-Agent": self.user_agent + " OpenTok-With-Vonage-API-Backend",
            "Authorization": "Bearer " + self._create_jwt_auth_header(),
            "Accept": "application/json",
        }

    def headers(self):
        warnings.warn(
            "opentok.headers is deprecated (use opentok.get_headers instead).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_headers()

    def get_json_headers(self):
        """For internal use."""
        result = self.get_headers()
        result["Content-Type"] = "application/json"
        return result

    def json_headers(self):
        warnings.warn(
            "opentok.json_headers is deprecated (use opentok.get_json_headers instead).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_json_headers()

    def start_archive(
        self,
        session_id,
        has_audio=True,
        has_video=True,
        name=None,
        output_mode=OutputModes.composed,
        stream_mode=StreamModes.auto,
        resolution=None,
        layout=None,
        multi_archive_tag=None,
        max_bitrate=None,
        quantization_parameter=None,
    ):
        """
        Starts archiving an OpenTok session.

        Clients must be actively connected to the OpenTok session for you to successfully start
        recording an archive.

        You can only record one archive at a time for a given session. You can only record archives
        of sessions that use the OpenTok Media Router (sessions with the media mode set to routed);
        you cannot archive sessions with the media mode set to relayed.

        For more information on archiving, see the
        `OpenTok archiving <https://tokbox.com/opentok/tutorials/archiving/>`_ programming guide.

        :param String session_id: The session ID of the OpenTok session to archive.
        :param String name: This is the name of the archive. You can use this name
          to identify the archive. It is a property of the Archive object, and it is a property
          of archive-related events in the OpenTok.js library.
        :param Boolean has_audio: if set to True, an audio track will be inserted to the archive.
          has_audio is an optional parameter that is set to True by default. If you set both
          has_audio and has_video to False, the call to the start_archive() method results in
          an error.
        :param Boolean has_video: if set to True, a video track will be inserted to the archive.
          has_video is an optional parameter that is set to True by default.
        :param OutputModes output_mode: Whether all streams in the archive are recorded
          to a single file (OutputModes.composed, the default) or to individual files
          (OutputModes.individual).
        :param String resolution (Optional): The resolution of the archive, either "640x480" (the default)
          or "1280x720". This parameter only applies to composed archives. If you set this
          parameter and set the output_mode parameter to OutputModes.individual, the call to the
          start_archive() method results in an error.
        :param Dictionary 'layout' optional: Specify this to assign the initial layout type for the
            archive.

            String 'type': Type of layout. Valid values for the layout property are "bestFit" (best fit),
            "custom" (custom), "horizontalPresentation" (horizontal presentation), "pip" (picture-in-picture),
            and "verticalPresentation" (vertical presentation)). If you specify a "custom" layout type, set
            the stylesheet property of the layout object to the stylesheet.

            String 'stylesheet' optional: Custom stylesheet to use for layout. Must set 'type' to custom,
            and cannot use 'screenshareType'

            String 'screenshareType' optional: Layout to use for screenshares. If this is set, you must
            set 'type' to 'bestFit'

        :param StreamModes stream_mode (Optional): Determines the archive stream handling mode.
        Set this to StreamModes.auto (the default) to have streams added automatically. Set this to
        StreamModes.manual to explicitly select streams to include in the the archive, using the
        OpenTok.add_archive_stream() and OpenTok.remove_archive_stream() methods.

        :param String multi_archive_tag (Optional): Set this to support recording multiple archives for the same
        session simultaneously. Set this to a unique string for each simultaneous archive of an ongoing session.
        You must also set this option when manually starting an archive that is automatically archived.
        Note that the multiArchiveTag value is not included in the response for the methods to list archives and
        retrieve archive information. If you do not specify a unique multi_archive_tag, you can only record one archive
        at a time for a given session.
        For more information, see simultaneous archives: https://tokbox.com/developer/guides/archiving/#simultaneous-archives.

        :param String max_bitrate (Optional): The maximum video bitrate for the archive, in bits per second. The minimum value is 100,000 and the maximum is 6,000,000.

        :param Number quantization_parameter (Optional): The quantization parameter (QP) for video encoding quality. Values between 15-40, where smaller values generate higher quality and larger archives, larger values generate lower quality and smaller archives. QP uses variable bitrate (VBR).

        :rtype: The Archive object, which includes properties defining the archive,
          including the archive ID.
        """
        if not isinstance(output_mode, OutputModes):
            raise OpenTokException(
                u("Cannot start archive, {0} is not a valid output mode").format(
                    output_mode
                )
            )

        if resolution and output_mode == OutputModes.individual:
            raise OpenTokException(
                u(
                    "Invalid parameters: Resolution cannot be supplied for individual output mode."
                )
            )

        if quantization_parameter is not None:
            if not isinstance(quantization_parameter, (int, float)):
                raise OpenTokException(
                    u("quantization_parameter must be a number")
                )
            if quantization_parameter < 15 or quantization_parameter > 40:
                raise OpenTokException(
                    u("quantization_parameter must be between 15 and 40")
                )

        payload = {
            "name": name,
            "sessionId": session_id,
            "hasAudio": has_audio,
            "hasVideo": has_video,
            "outputMode": output_mode.value,
            "resolution": resolution,
            "streamMode": stream_mode.value,
            "multiArchiveTag": multi_archive_tag,
            "maxBitrate": max_bitrate,
        }

        if quantization_parameter is not None:
            payload["quantizationParameter"] = quantization_parameter

        if layout is not None:
            payload["layout"] = layout

        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            self.endpoints.get_archive_url(),
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_archive_url(),
            data=json.dumps(payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            return Archive(self, response.json())
        elif response.status_code == 403:
            raise AuthError()
        elif response.status_code == 400:
            """
            The HTTP response has a 400 status code in the following cases:
            You do not pass in a session ID or you pass in an invalid session ID.
            No clients are actively connected to the OpenTok session.
            You specify an invalid resolution value.
            The outputMode property is set to "individual" and you set the resolution property and (which is not supported in individual stream archives).
            """
            raise RequestError(response.json().get("message"))
        elif response.status_code == 404:
            raise NotFoundError("Session not found")
        elif response.status_code == 409:
            raise ArchiveError(response.json().get("message"))
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def stop_archive(self, archive_id):
        """
        Stops an OpenTok archive that is being recorded.

        Archives automatically stop recording after 90 minutes or when all clients have disconnected
        from the session being archived.

        @param [String] archive_id The archive ID of the archive you want to stop recording.

        :rtype: The Archive object corresponding to the archive being stopped.
        """
        logger.debug(
            "POST to %r with headers %r, proxies %r",
            self.endpoints.get_archive_url(archive_id) + "/stop",
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_archive_url(archive_id) + "/stop",
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code < 300:
            return Archive(self, response.json())
        elif response.status_code == 403:
            raise AuthError()
        elif response.status_code == 404:
            raise NotFoundError("Archive not found")
        elif response.status_code == 409:
            raise ArchiveError("Archive is not in started state")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def delete_archive(self, archive_id):
        """
        Deletes an OpenTok archive.

        You can only delete an archive which has a status of "available" or "uploaded". Deleting an
        archive removes its record from the list of archives. For an "available" archive, it also
        removes the archive file, making it unavailable for download.

        :param String archive_id: The archive ID of the archive to be deleted.
        """
        logger.debug(
            "DELETE to %r with headers %r, proxies %r",
            self.endpoints.get_archive_url(archive_id),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.delete(
            self.endpoints.get_archive_url(archive_id),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code < 300:
            pass
        elif response.status_code == 403:
            raise AuthError()
        elif response.status_code == 404:
            raise NotFoundError("Archive not found")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def get_archive(self, archive_id):
        """Gets an Archive object for the given archive ID.

        :param String archive_id: The archive ID.

        :rtype: The Archive object.
        """
        logger.debug(
            "GET to %r with headers %r, proxies %r",
            self.endpoints.get_archive_url(archive_id),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.get(
            self.endpoints.get_archive_url(archive_id),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            return Archive(self, response.json())
        elif response.status_code == 403:
            raise AuthError()
        elif response.status_code == 404:
            raise NotFoundError("Archive not found")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def get_archives(self, offset=None, count=None, session_id=None):
        """Returns an ArchiveList, which is an array of archives that are completed and in-progress,
        for your API key.

        :param int: offset Optional. The index offset of the first archive. 0 is offset
          of the most recently started archive. 1 is the offset of the archive that started prior to
          the most recent archive. If you do not specify an offset, 0 is used.
        :param int: count Optional. The number of archives to be returned. The maximum
          number of archives returned is 1000.
        :param string: session_id Optional. Used to list archives for a specific session ID.

        :rtype: An ArchiveList object, which is an array of Archive objects.
        """
        params = {}
        if offset is not None:
            params["offset"] = offset
        if count is not None:
            params["count"] = count
        if session_id is not None:
            params["sessionId"] = session_id

        endpoint = self.endpoints.get_archive_url() + "?" + urlencode(params)

        logger.debug(
            "GET to %r with headers %r, proxies %r",
            endpoint,
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.get(
            endpoint,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            return ArchiveList(self, response.json())
        elif response.status_code == 403:
            raise AuthError()
        elif response.status_code == 404:
            raise NotFoundError("Archive not found")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def list_archives(self, offset=None, count=None, session_id=None):
        """
        New method to get archive list, it's alternative to 'get_archives()',
        both methods exist to have backwards compatible
        """
        return self.get_archives(offset, count, session_id)

    def add_archive_stream(
        self,
        archive_id: str,
        stream_id: str,
        has_audio: bool = True,
        has_video: bool = True,
    ) -> requests.Response:
        """
        This method will add streams to the archive with addStream for new participant(choosing audio, video or both).

        :param String archive_id: the ID of the archive that will be updated
        :param String stream_id: the id of the stream that will get added to the archive
        :param Boolean has_audio: if set to True, an audio track will be inserted to the archive.
          has_audio is an optional parameter that is set to True by default. If you set both
          has_audio and has_video to False, the call to the add_archive_stream() method results in
          an error.
        :param Boolean has_video: if set to True, a video track will be inserted to the archive.
          has_video is an optional parameter that is set to True by default.
        """

        endpoint = self.endpoints.get_archive_stream(archive_id)

        streams = {"hasAudio": has_audio, "hasVideo": has_video, "addStream": stream_id}

        response = requests.patch(
            endpoint,
            data=json.dumps(streams),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            if response.status_code == 204:
                return None
            elif response.status_code == 403:
                raise AuthError()
            elif response.status_code == 400:
                """
                The HTTP response has a 400 status code in the following cases:
                You do not pass in a session ID or you pass in an invalid session ID.
                No clients are actively connected to the OpenTok session.
                You specify an invalid resolution value.
                The outputMode property is set to "individual" and you set the resolution property and (which is not supported in individual stream archives).
                """
                raise RequestError(response.json().get("message"))
            elif response.status_code == 404:
                raise NotFoundError("Archive or Stream not found")
            elif response.status_code == 405:
                raise ArchiveStreamModeError(
                    "Your archive is configured with a streamMode that does not support stream manipulation."
                )
            elif response.status_code == 409:
                raise ArchiveError(response.json().get("message"))
        else:
            raise RequestError("An unexpected error occurred.", response.status_code)

    def remove_archive_stream(self, archive_id: str, stream_id: str) -> requests.Response:
        """
        This method will remove streams from the archive with removeStream.

        :param String archive_id: the ID of the archive that will be updated
        :param String stream_id: the ID of the stream that will get added to the archive
        """

        endpoint = self.endpoints.get_archive_stream(archive_id)

        streams = {"removeStream": stream_id}

        response = requests.patch(
            endpoint,
            data=json.dumps(streams),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            if response.status_code == 204:
                return None
            elif response.status_code == 403:
                raise AuthError()
            elif response.status_code == 400:
                """
                The HTTP response has a 400 status code in the following cases:
                You do not pass in a session ID or you pass in an invalid session ID.
                No clients are actively connected to the OpenTok session.
                You specify an invalid resolution value.
                The outputMode property is set to "individual" and you set the resolution property and (which is not supported in individual stream archives).
                """
                raise RequestError(response.json().get("message"))
            elif response.status_code == 404:
                raise NotFoundError("Archive or Stream not found")
            elif response.status_code == 405:
                raise ArchiveStreamModeError(
                    "Your archive is configured with a streamMode that does not support stream manipulation."
                )
            elif response.status_code == 409:
                raise ArchiveError(response.json().get("message"))
        else:
            raise RequestError("An unexpected error occurred.", response.status_code)

    def send_signal(self, session_id, payload, connection_id=None):
        """
        Send signals to all participants in an active OpenTok session or to a specific client
        connected to that session.

        :param String session_id: The session ID of the OpenTok session that receives the signal

        :param Dictionary payload: Structure that contains both the type and data fields. These
        correspond to the type and data parameters passed in the client signal received handlers

        :param String connection_id: The connection_id parameter is an optional string used to
        specify the connection ID of a client connected to the session. If you specify this value,
        the signal is sent to the specified client. Otherwise, the signal is sent to all clients
        connected to the session
        """
        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            self.endpoints.get_signaling_url(session_id, connection_id),
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_signaling_url(session_id, connection_id),
            data=json.dumps(payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            if response.status_code == 204:
                return None
            elif response.status_code == 400:
                raise SignalingError(
                    "One of the signal properties - data, type, sessionId or connectionId - is invalid."
                )
            elif response.status_code == 403:
                raise AuthError(
                    "You are not authorized to send the signal. Check your authentication credentials."
                )
            elif response.status_code == 404:
                raise SignalingError(
                    "The client specified by the connectionId property is not connected to the session."
                )
            elif response.status_code == 413:
                raise SignalingError(
                    "The type string exceeds the maximum length (128 bytes), or the data string exceeds the maximum size (8 kB)."
                )
        else:
            raise RequestError("An unexpected error occurred.", response.status_code)

    def signal(self, session_id, payload, connection_id=None):
        warnings.warn(
            "opentok.signal is deprecated (use opentok.send_signal instead).",
            DeprecationWarning,
            stacklevel=2,
        )
        self.send_signal(session_id, payload, connection_id)

    def get_stream(self, session_id, stream_id):
        """
        Returns an Stream object that contains information of an OpenTok stream:

        -id: The stream ID
        -videoType: "camera" or "screen"
        -name: The stream name (if one was set when the client published the stream)
        -layoutClassList: It's an array of the layout classes for the stream
        """
        endpoint = self.endpoints.get_stream_url(session_id, stream_id)

        logger.debug(
            "GET to %r with headers %r, proxies %r",
            endpoint,
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.get(
            endpoint,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return Stream(response.json())
        elif response.status_code == 400:
            raise GetStreamError(
                "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
            )
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        elif response.status_code == 408:
            raise GetStreamError("You passed in an invalid stream ID.")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def list_streams(self, session_id):
        """
        Returns a list of Stream objects that contains information of all
        the streams in a OpenTok session, with the following attributes:

        -count: An integer that indicates the number of streams in the session
        -items: List of the Stream objects
        """
        endpoint = self.endpoints.get_stream_url(session_id)

        logger.debug(
            "GET to %r with headers %r, proxies %r",
            endpoint,
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.get(
            endpoint,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return StreamList(response.json())
        elif response.status_code == 400:
            raise GetStreamError(
                "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
            )
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        elif response.status_code == 404:
            return StreamList({"count": 0, "items": []})
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def force_disconnect(self, session_id, connection_id):
        """
        Sends a request to disconnect a client from an OpenTok session

        :param String session_id: The session ID of the OpenTok session from which the
        client will be disconnected

        :param String connection_id: The connection ID of the client that will be disconnected
        """
        endpoint = self.endpoints.force_disconnect_url(session_id, connection_id)

        logger.debug(
            "DELETE to %r with headers %r, proxies %r",
            endpoint,
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.delete(
            endpoint,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 204:
            pass
        elif response.status_code == 400:
            raise ForceDisconnectError(
                "One of the arguments - sessionId or connectionId - is invalid."
            )
        elif response.status_code == 403:
            raise AuthError(
                "You are not authorized to forceDisconnect, check your authentication credentials."
            )
        elif response.status_code == 404:
            raise ForceDisconnectError(
                "The client specified by the connectionId property is not connected to the session."
            )
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def set_archive_layout(
        self, archive_id, layout_type, stylesheet=None, screenshare_type=None
    ):
        """
        Use this method to change the layout of videos in an OpenTok archive

        :param String archive_id: The ID of the archive that will be updated

        :param String layout_type: The layout type for the archive. Valid values are:
        'bestFit', 'custom', 'horizontalPresentation', 'pip' and 'verticalPresentation'

        :param String stylesheet optional: CSS used to style the custom layout.
        Specify this only if you set the type property to 'custom'

        :param String screenshare_type optional: Layout to use for screenshares. Must
        set 'layout_type' to 'bestFit'
        """
        payload = {
            "type": layout_type,
        }

        if screenshare_type is not None:
            payload["screenshareType"] = screenshare_type

        if layout_type == "custom":
            if stylesheet is not None:
                payload["stylesheet"] = stylesheet

        endpoint = self.endpoints.set_archive_layout_url(archive_id)

        logger.debug(
            "PUT to %r with params %r, headers %r, proxies %r",
            endpoint,
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.put(
            endpoint,
            data=json.dumps(payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            pass
        elif response.status_code == 400:
            raise ArchiveError(
                "Invalid request. This response may indicate that data in your request data is invalid JSON. It may also indicate that you passed in invalid layout options."
            )
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def dial(self, session_id, token, sip_uri, options={}):
        """
        Use this method to connect a SIP platform to an OpenTok session. The audio from the end
        of the SIP call is added to the OpenTok session as an audio-only stream. The OpenTok Media
        Router mixes audio from other streams in the session and sends the mixed audio to the SIP
        endpoint

        :param String session_id: The OpenTok session ID for the SIP call to join

        :param String token: The OpenTok token to be used for the participant being called

        :param String sip_uri: The SIP URI to be used as destination of the SIP call initiated from
        OpenTok to the SIP platform

        :param Dictionary options optional: Aditional options with the following properties:

            String 'from': The number or string that will be sent to the final SIP number
            as the caller

            Dictionary 'headers': Defines custom headers to be added to the SIP INVITE request
            initiated from OpenTok to the SIP platform

            Dictionary 'auth': Contains the username and password to be used in the the SIP
            INVITE request for HTTP digest authentication, if it is required by the SIP platform
            For example:

                'auth': {
                    'username': 'username',
                    'password': 'password'
                }

            Boolean 'secure': A Boolean flag that indicates whether the media must be transmitted
            encrypted (true) or not (False, the default)

            Boolean 'observeForceMute': Whether the SIP endpoint should honor a force mute action
            (True) or not (False, the default). A force mute action allows a moderator to force clients to
            mute audio in streams they publish.

            Boolean 'video': Whether the SIP call will include video (true)
            or not (False, the default). With video included, the SIP client's video is included
            in the OpenTok stream that is sent to the OpenTok session. The SIP client will receive a single
            composed video of the published streams in the OpenTok session.

            List 'streams': An array of stream IDs for streams to include in the SIP call.
            If you do not set this property, all streams in the session are included in the call.

            This is an example of what the payload POST data dictionary could look like:

            {
                "sessionId": "Your OpenTok session ID",
                "token": "Your valid OpenTok token",
                "sip": {
                    "uri": "sip:user@sip.partner.com;transport=tls",
                    "from": "from@example.com",
                    "headers": {
                        "headerKey": "headerValue"
                    },
                    "auth": {
                        "username": "username",
                        "password": "password"
                    },
                    "secure": True,
                    "video": True,
                    "observeForceMute": True,
                    "streams": ["stream-id-1", "stream-id-2"]
                }
            }

        :rtype: A SipCall object, which contains data of the SIP call: id, connectionId and streamId.
        This is what the response body should look like after returning with a status code of 200:

        {
            "id": "b0a5a8c7-dc38-459f-a48d-a7f2008da853",
            "connectionId": "e9f8c166-6c67-440d-994a-04fb6dfed007",
            "streamId": "482bce73-f882-40fd-8ca5-cb74ff416036",
        }

        Note: Your response will have a different: id, connectionId and streamId
        """

        payload = {
            "sessionId": session_id,
            "token": token,
            "sip": {"uri": sip_uri, **options},
        }

        endpoint = self.endpoints.dial_url()

        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            endpoint,
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            endpoint,
            data=json.dumps(payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return SipCall(response.json())
        elif response.status_code == 400:
            raise SipDialError("Invalid request. Invalid session ID.")
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        elif response.status_code == 404:
            raise SipDialError("The session does not exist.")
        elif response.status_code == 409:
            raise SipDialError(
                "You attempted to start a SIP call for a session that "
                "does not use the OpenTok Media Router."
            )
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def set_stream_class_lists(self, session_id, payload):
        """
        Use this method to change layout classes for OpenTok streams. The layout classes
        define how the streams are displayed in the layout of a composed OpenTok archive

        :param String session_id: The ID of the session of the streams that will be updated

        :param List payload: A list defining the class lists to apply to the streams.
        Each element in the list is a dictionary with two properties: 'id' and 'layoutClassList'.
        The 'id' property is the stream ID (a String), and the 'layoutClassList' is an array of
        class names (Strings) to apply to the stream. For example:

            payload = [
                {'id': '7b09ec3c-26f9-43d7-8197-f608f13d4fb6', 'layoutClassList': ['focus']},
                {'id': '567bc941-6ea0-4c69-97fc-70a740b68976', 'layoutClassList': ['top']},
                {'id': '307dc941-0450-4c09-975c-705740d08970', 'layoutClassList': ['bottom']}
            ]
        """
        items_payload = {"items": payload}

        endpoint = self.endpoints.set_stream_class_lists_url(session_id)

        logger.debug(
            "PUT to %r with params %r, headers %r, proxies %r",
            endpoint,
            json.dumps(items_payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.put(
            endpoint,
            data=json.dumps(items_payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            pass
        elif response.status_code == 400:
            raise SetStreamClassError(
                "Invalid request. This response may indicate that data in your request data "
                "is invalid JSON. It may also indicate that you passed in invalid layout options."
            )
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def start_broadcast(self, session_id, options, stream_mode=BroadcastStreamModes.auto):
        """
        Use this method to start a live streaming broadcast for an OpenTok session. This broadcasts
        the session to an HLS (HTTP live streaming) or to RTMP streams. To successfully start
        broadcasting a session, at least one client must be connected to the session. You can only
        start live streaming for sessions that use the OpenTok Media Router (with the media mode set
        to routed); you cannot use live streaming with sessions that have the media mode set to
        relayed

        :param String session_id: The session ID of the OpenTok session you want to broadcast

        :param Dictionary options, with the following properties:

            :param Boolean optional hasAudio: Whether the stream is broadcast with audio.

            :param Boolean optional hasVideo: Whether the stream is broadcast with video.

            Dictionary 'layout' optional: Specify this to assign the initial layout type for the
            broadcast.

                String 'type': Type of layout. Valid values for the layout property are "bestFit" (best fit),
                "custom" (custom), "horizontalPresentation" (horizontal presentation), "pip" (picture-in-picture),
                and "verticalPresentation" (vertical presentation)). If you specify a "custom" layout type, set
                the stylesheet property of the layout object to the stylesheet.

                String 'stylesheet' optional: Custom stylesheet to use for layout. Must set 'type' to custom,
                and cannot use 'screenshareType'

                String 'screenshareType' optional: Layout to use for screenshares. If this is set, you must
                set 'type' to 'bestFit'

            Integer 'maxDuration' optional: The maximum duration for the broadcast, in seconds.
            The broadcast will automatically stop when the maximum duration is reached. You can
            set the maximum duration to a value from 60 (60 seconds) to 36000 (10 hours). The
            default maximum duration is 4 hours (14,400 seconds)

            Integer 'maxBitrate' optional: The maximum bitrate (bits per second) used by the broadcast.
            Value must be between 100_000 and 6_000_000.

            Dictionary 'outputs': This object defines the types of broadcast streams you want to
            start (both HLS and RTMP). You can include HLS, RTMP, or both as broadcast streams.
            If you include RTMP streaming, you can specify up to five target RTMP streams. For
            each RTMP stream, specify 'serverUrl' (the RTMP server URL), 'streamName' (the stream
            name, such as the YouTube Live stream name or the Facebook stream key), and
            (optionally) 'id' (a unique ID for the stream). You can optionally specify lowLatency or
            DVR mode, but these options are mutually exclusive. For example:

                'outputs': {
                    'hls': {
                        'lowLatency': True,
                        'dvr': False
                    },
                    'rtmp': [
                        {
                            'id': 'foo',
                            'serverUrl': 'rtmp://myfooserver/myfooapp',
                            'streamName': 'myfoostream'
                    }, {
                        'id': 'bar',
                        'serverUrl': 'rtmp://mybarserver/mybarapp',
                        'streamName': 'mybarstream'
                    }]
                }

            String 'resolution' optional: The resolution of the broadcast, either "640x480"
            (SD, the default) or "1280x720" (HD)

            String 'multiBroadcastTag' optional: Set this to support multiple broadcasts for the same session simultaneously.
            Set this to a unique string for each simultaneous broadcast of an ongoing session.
            Note that the multiBroadcastTag value is not included in the response for the methods to list live streaming
            broadcasts and get information about a live streaming broadcast.
            For more information, see https://tokbox.com/developer/guides/broadcast/live-streaming#simultaneous-broadcasts.

        :param BroadcastStreamModes stream_mode (Optional): Determines the broadcast stream handling mode.
        Set this to BroadcastStreamModes.auto (the default) to have streams added automatically. Set this to
        BroadcastStreamModes.manual to explicitly select streams to include in the the broadcast, using the
        OpenTok.add_broadcast_stream() and OpenTok.remove_broadcast_stream() methods.

        :rtype A Broadcast object, which contains information of the broadcast: id, sessionId,
        projectId, createdAt, updatedAt, resolution, status and broadcastUrls
        """

        if "hls" in options["outputs"]:
            if (
                "lowLatency" in options["outputs"]["hls"]
                and "dvr" in options["outputs"]["hls"]
            ):
                if (
                    options["outputs"]["hls"]["lowLatency"] == True
                    and options["outputs"]["hls"]["dvr"] == True
                ):
                    raise BroadcastHLSOptionsError(
                        'HLS options "lowLatency" and "dvr" cannot both be set to "True".'
                    )

        if "maxBitrate" in options:
            if (
                type(options["maxBitrate"]) != int
                or options["maxBitrate"] < 100000
                or options["maxBitrate"] > 6000000
            ):
                raise BroadcastOptionsError(
                    "maxBitrate must be an integer between 100000 and 6000000."
                )

        payload = {"sessionId": session_id, "streamMode": stream_mode.value}

        payload.update(options)

        endpoint = self.endpoints.get_broadcast_url()

        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            endpoint,
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            endpoint,
            data=json.dumps(payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            return Broadcast(response.json())
        elif response.status_code == 400:
            raise BroadcastError(
                "Invalid request. This response may indicate that data in your request data is "
                "invalid JSON. It may also indicate that you passed in invalid layout options. "
                "Or you have exceeded the limit of five simultaneous RTMP streams for an OpenTok "
                "session. Or you specified and invalid resolution."
            )
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        elif response.status_code == 409:
            raise BroadcastError("The broadcast has already started for the session.")
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def stop_broadcast(self, broadcast_id):
        """
        Use this method to stop a live broadcast of an OpenTok session

        :param String broadcast_id: The ID of the broadcast you want to stop

        :rtype A Broadcast object, which contains information of the broadcast: id, sessionId
        projectId, createdAt, updatedAt and resolution
        """

        endpoint = self.endpoints.get_broadcast_url(broadcast_id, stop=True)

        logger.debug(
            "POST to %r with headers %r, proxies %r",
            endpoint,
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            endpoint,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return Broadcast(response.json())
        elif response.status_code == 400:
            raise BroadcastError(
                "Invalid request. This response may indicate that data in your request "
                "data is invalid JSON."
            )
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        elif response.status_code == 409:
            raise BroadcastError(
                "The broadcast (with the specified ID) was not found or it has already "
                "stopped."
            )
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def add_broadcast_stream(
        self,
        broadcast_id: str,
        stream_id: str,
        has_audio: bool = True,
        has_video: bool = True,
    ) -> requests.Response:
        """
        This method will add streams to the broadcast with addStream for new participant(choosing audio, video or both).

        :param String broadcast_id: the ID of the broadcast that will be updated
        :param String stream_id: the id of the stream that will get added to the broadcast
        :param Boolean has_audio: if set to True, an audio track will be inserted to the broadcast.
          has_audio is an optional parameter that is set to True by default. If you set both
          has_audio and has_video to False, the call to the add_broadcast_stream() method results in
          an error.
        :param Boolean has_video: if set to True, a video track will be inserted to the broadcast.
          has_video is an optional parameter that is set to True by default.
        """

        endpoint = self.endpoints.get_broadcast_stream(broadcast_id)

        streams = {"hasAudio": has_audio, "hasVideo": has_video, "addStream": stream_id}

        response = requests.patch(
            endpoint,
            data=json.dumps(streams),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            if response.status_code == 204:
                return None
            elif response.status_code == 400:
                raise BroadcastError(
                    "Invalid request. This response may indicate that data in your request data is "
                    "invalid JSON. It may also indicate that you passed in invalid layout options. "
                    "Or you have exceeded the limit of five simultaneous RTMP streams for an OpenTok "
                    "session. Or you specified and invalid resolution."
                )
            elif response.status_code == 403:
                raise AuthError("Authentication error.")
            elif response.status_code == 405:
                raise BroadcastStreamModeError(
                    "Your broadcast is configured with a streamMode that does not support stream manipulation."
                )
            elif response.status_code == 409:
                raise BroadcastError("The broadcast has already started for the session.")
        else:
            raise RequestError("An unexpected error occurred.", response.status_code)

    def remove_broadcast_stream(
        self, broadcast_id: str, stream_id: str
    ) -> requests.Response:
        """
        This method will remove streams from the broadcast with removeStream.

        :param String broadcast_id: the ID of the broadcast that will be updated
        :param String stream_id: the id of the stream that will get added to the broadcast
        """

        endpoint = self.endpoints.get_broadcast_stream(broadcast_id)

        streams = {"removeStream": stream_id}

        response = requests.patch(
            endpoint,
            data=json.dumps(streams),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response:
            if response.status_code == 204:
                return None
            elif response.status_code == 400:
                raise BroadcastError(
                    "Invalid request. This response may indicate that data in your request data is "
                    "invalid JSON. It may also indicate that you passed in invalid layout options. "
                    "Or you have exceeded the limit of five simultaneous RTMP streams for an OpenTok "
                    "session. Or you specified and invalid resolution."
                )
            elif response.status_code == 403:
                raise AuthError("Authentication error.")
            elif response.status_code == 405:
                raise BroadcastStreamModeError(
                    "Your broadcast is configured with a streamMode that does not support stream manipulation."
                )
            elif response.status_code == 409:
                raise BroadcastError("The broadcast has already started for the session.")
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def get_broadcast(self, broadcast_id):
        """
        Use this method to get details on a broadcast that is in-progress.

        :param String broadcast_id: The ID of the broadcast you want to stop

        :rtype A Broadcast object, which contains information of the broadcast: id, sessionId
        projectId, createdAt, updatedAt, resolution, broadcastUrls and status
        """

        endpoint = self.endpoints.get_broadcast_url(broadcast_id)

        logger.debug(
            "GET to %r with headers %r, proxies %r",
            endpoint,
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.get(
            endpoint,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return Broadcast(response.json())
        elif response.status_code == 400:
            raise BroadcastError(
                "Invalid request. This response may indicate that data in your request "
                "data is invalid JSON."
            )
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        elif response.status_code == 409:
            raise BroadcastError("No matching broadcast found (with the specified ID).")
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def set_broadcast_layout(
        self, broadcast_id, layout_type, stylesheet=None, screenshare_type=None
    ):
        """
        Use this method to change the layout type of a live streaming broadcast

        :param String broadcast_id: The ID of the broadcast that will be updated

        :param String layout_type: The layout type for the broadcast. Valid values are:
        'bestFit', 'custom', 'horizontalPresentation', 'pip' and 'verticalPresentation'

        :param String stylesheet optional: CSS used to style the custom layout.
        Specify this only if you set the type property to 'custom'

        :param String screenshare_type optional: Layout to use for screenshares. Must
        set 'layout_type' to 'bestFit'
        """
        payload = {
            "type": layout_type,
        }

        if screenshare_type is not None:
            payload["screenshareType"] = screenshare_type

        if layout_type == "custom":
            if stylesheet is not None:
                payload["stylesheet"] = stylesheet

        endpoint = self.endpoints.get_broadcast_url(broadcast_id, layout=True)

        logger.debug(
            "PUT to %r with params %r, headers %r, proxies %r",
            endpoint,
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.put(
            endpoint,
            data=json.dumps(payload),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            pass
        elif response.status_code == 400:
            raise BroadcastError(
                "Invalid request. This response may indicate that data in your request data is "
                "invalid JSON. It may also indicate that you passed in invalid layout options."
            )
        elif response.status_code == 403:
            raise AuthError("Authentication error.")
        else:
            raise RequestError("OpenTok server error.", response.status_code)

    def start_render(
        self,
        session_id,
        opentok_token,
        url,
        max_duration=7200,
        resolution="1280x720",
        properties: dict = None,
    ):
        """
         Starts an Experience Composer for the specified OpenTok session.
         For more information, see the
        `Experience Composer developer guide <https://tokbox.com/developer/guides/experience-composer>`_.

         :param String 'session_id': The session ID of the OpenTok session that will include the Experience Composer stream.
         :param String 'opentok_token': A valid OpenTok token with a Publisher role and (optionally) connection data to be associated with the output stream.
         :param String 'url': A publically reachable URL controlled by the customer and capable of generating the content to be rendered without user intervention.
         :param Integer 'maxDuration' Optional: The maximum time allowed for the Experience Composer, in seconds. After this time, it is stopped automatically, if it is still running. The maximum value is 36000 (10 hours), the minimum value is 60 (1 minute), and the default value is 7200 (2 hours). When the Experience Composer ends, its stream is unpublished and an event is posted to the callback URL, if configured in the Account Portal.
         :param String 'resolution' Optional: The resolution of the Experience Composer, either "640x480" (SD landscape), "480x640" (SD portrait), "1280x720" (HD landscape), "720x1280" (HD portrait), "1920x1080" (FHD landscape), or "1080x1920" (FHD portrait). By default, this resolution is "1280x720" (HD landscape, the default).
         :param Dictionary 'properties' Optional: Initial configuration of Publisher properties for the composed output stream.
             String name Optional: The name of the composed output stream which will be published to the session. The name must have a minimum length of 1 and a maximum length of 200.
        """
        payload = {
            "sessionId": session_id,
            "token": opentok_token,
            "url": url,
            "maxDuration": max_duration,
            "resolution": resolution,
            "properties": properties,
        }

        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            self.endpoints.get_render_url(),
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_render_url(),
            json=payload,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response and response.status_code == 202:
            return Render(response.json())
        elif response.status_code == 400:
            """
            The HTTP response has a 400 status code in the following cases:
            You did not pass in a session ID or you passed in an invalid session ID.
            You specify an invalid value for input parameters.
            """
            raise RequestError(response.json().get("message"))
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def get_render(self, render_id):
        """
        This method allows you to see the status of a render, which can be one of the following:
            ['starting', 'started', 'stopped', 'failed']

        :param String 'render_id': The ID of a specific render.
        """
        logger.debug(
            "GET to %r with headers %r, proxies %r",
            self.endpoints.get_render_url(render_id=render_id),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.get(
            self.endpoints.get_render_url(render_id=render_id),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return Render(response.json())
        elif response.status_code == 400:
            raise RequestError(
                "Invalid request. This response may indicate that data in your request is invalid JSON. Or it may indicate that you did not pass in a session ID."
            )
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        elif response.status_code == 404:
            raise NotFoundError("No render matching the specified render ID was found.")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def stop_render(self, render_id):
        """
        This method stops a render.

        :param String 'render_id': The ID of a specific render.
        """
        logger.debug(
            "DELETE to %r with headers %r, proxies %r",
            self.endpoints.get_render_url(render_id=render_id),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.delete(
            self.endpoints.get_render_url(render_id=render_id),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return response
        elif response.status_code == 400:
            raise RequestError(
                "Invalid request. This response may indicate that data in your request is invalid JSON. Or it may indicate that you did not pass in a session ID."
            )
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        elif response.status_code == 404:
            raise NotFoundError("No render matching the specified render ID was found.")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def list_renders(self, offset=0, count=50):
        """
        List existing renders associated with the project's API key.

        :param Integer 'offset' Optional: Start offset in the list of existing renders.
        :param Integer 'count' Optional: Number of renders to retrieve, starting at 'offset'.
        """

        query_params = {"offset": offset, "count": count}

        logger.debug(
            "GET to %r with headers %r, params %r, proxies %r",
            self.endpoints.get_render_url(),
            self.get_json_headers(),
            query_params,
            self.proxies,
        )

        response = requests.get(
            self.endpoints.get_render_url(),
            headers=self.get_json_headers(),
            params=query_params,
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response.status_code == 200:
            return RenderList(self, response.json())
        elif response.status_code == 400:
            raise RequestError(
                "Invalid request. This response may indicate that data in your request is invalid JSON. Or it may indicate that you do not pass in a session ID."
            )
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def connect_audio_to_websocket(
        self, session_id: str, opentok_token: str, websocket_options: dict
    ):
        """
        Connects audio streams to a specified WebSocket URI.
        For more information, see the `Audio Connector developer guide <https://tokbox.com/developer/guides/audio-streamer/>`.

        :param String 'session_id': The OpenTok session ID that includes the OpenTok streams you want to include in the WebSocket stream. The Audio Connector feature is only supported in routed sessions (sessions that use the OpenTok Media Router).
        :param String 'opentok_token': The OpenTok token to be used for the Audio Connector connection to the OpenTok session.
        :param Dictionary 'websocket_options': Included options for the WebSocket.
            String 'uri': A publicly reachable WebSocket URI to be used for the destination of the audio stream (such as "wss://example.com/ws-endpoint").
            List 'streams' Optional: A list of stream IDs for the OpenTok streams you want to include in the WebSocket audio. If you omit this property, all streams in the session will be included.
            Dictionary 'headers' Optional: An object of key-value pairs of headers to be sent to your WebSocket server with each message, with a maximum length of 512 bytes.
            Boolean 'bidirectional' Optional: If true, enables bidirectional audio streaming over the WebSocket connection.
        """
        self.validate_websocket_options(websocket_options)

        payload = {
            "sessionId": session_id,
            "token": opentok_token,
            "websocket": websocket_options,
        }

        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            self.endpoints.get_audio_connector_url(),
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_audio_connector_url(),
            json=payload,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response and response.status_code == 200:
            return WebSocketAudioConnection(response.json())
        elif response.status_code == 400:
            """
            The HTTP response has a 400 status code in the following cases:
            You did not pass in a session ID or you passed in an invalid session ID.
            You specified an invalid value for input parameters.
            """
            raise RequestError(response.json().get("message"))
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT token.")
        elif response.status_code == 409:
            raise InvalidMediaModeError(
                "Only routed sessions are allowed to initiate Audio Connector WebSocket connections."
            )
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def validate_websocket_options(self, options):
        if type(options) is not dict:
            raise InvalidWebSocketOptionsError(
                "Must pass WebSocket options as a dictionary."
            )
        if "uri" not in options:
            raise InvalidWebSocketOptionsError("Provide a WebSocket URI.")

        if "bidirectional" in options:
            if not isinstance(options["bidirectional"], bool):
                raise InvalidWebSocketOptionsError("'bidirectional' must be a boolean if provided.")

    def start_captions(
        self,
        session_id: str,
        opentok_token: str,
        language_code: str = "en-US",
        max_duration: int = 14400,
        partial_captions: bool = True,
        status_callback_url: str = None,
    ):
        """
        Starts real-time Live Captions for an OpenTok Session. The maximum allowed duration is 4 hours, after which the audio
        captioning will stop without any effect on the ongoing OpenTok Session.
        An event will be posted to your callback URL if provided when starting the captions.

        Each OpenTok Session supports only one audio captioning session. For more information about the Live Captions feature,
        see the Live Captions developer guide <https://tokbox.com/developer/guides/live-captions/>.

        :param String 'session_id': The OpenTok session ID. The audio from participants publishing into this session will be used to generate the captions.
        :param String 'opentok_token': A valid OpenTok token with role set to Moderator.
        :param String 'language_code' Optional: The BCP-47 code for a spoken language used on this call.
        :param Integer 'max_duration' Optional: The maximum duration for the audio captioning, in seconds.
        :param Boolean 'partial_captions' Optional: Whether to enable this to faster captioning at the cost of some inaccuracies.
        :param String 'status_callback_url' Optional: A publicly reachable URL controlled by the customer and capable of generating the content to be rendered without user intervention. The minimum length of the URL is 15 characters and the maximum length is 2048 characters.
        """

        payload = {
            "sessionId": session_id,
            "token": opentok_token,
            "languageCode": language_code,
            "maxDuration": max_duration,
            "partialCaptions": partial_captions,
            "statusCallbackUrl": status_callback_url,
        }

        logger.debug(
            "POST to %r with params %r, headers %r, proxies %r",
            self.endpoints.get_captions_url(),
            json.dumps(payload),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_captions_url(),
            json=payload,
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        # Keeping backwards compat just in case
        if response and response.status_code == 200:
            return Captions(response.json())
        if response and response.status_code == 202:
            return Captions(response.json())
        elif response.status_code == 400:
            """
            The HTTP response has a 400 status code in the following cases:
            You did not pass in a session ID or you passed in an invalid session ID.
            You specified an invalid value for input parameters.
            """
            raise RequestError(response.json().get("message"))
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT.")
        elif response.status_code == 409:
            raise CaptioningAlreadyInProgressError(
                "Live captions have already started for this OpenTok Session."
            )
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def stop_captions(self, captions_id: str):
        """
        Stops live captioning for the specified captioning session.

        :param String captions_id: The ID of the captioning session to stop.
        """

        logger.debug(
            "POST to %r with headers %r, proxies %r",
            self.endpoints.get_captions_url(captions_id),
            self.get_json_headers(),
            self.proxies,
        )

        response = requests.post(
            self.endpoints.get_captions_url(captions_id),
            headers=self.get_json_headers(),
            proxies=self.proxies,
            timeout=self.timeout,
        )

        if response and response.status_code == 202:
            return None
        elif response.status_code == 403:
            raise AuthError("You passed in an invalid OpenTok API key or JWT.")
        elif response.status_code == 404:
            raise NotFoundError("No matching captionsId was found.")
        else:
            raise RequestError("An unexpected error occurred", response.status_code)

    def _sign_string(self, string, secret):
        return hmac.new(
            secret.encode("utf-8"), string.encode("utf-8"), hashlib.sha1
        ).hexdigest()

    def _create_jwt_auth_header(self):
        payload = {
            "ist": "project",
            "iat": int(time.time()),  # current time in unix time (seconds)
            "exp": int(time.time())
            + (60 * self._jwt_livetime),  # 3 minutes in the future (seconds)
        }

        if not self._using_vonage:
            payload["iss"] = self.api_key
            payload["jti"] = str(random.random())
            return encode(payload, self.api_secret, algorithm="HS256")

        payload["application_id"] = self.api_key
        payload["jti"] = str(uuid.uuid4())
        headers = {"typ": "JWT", "alg": "RS256"}

        return encode(payload, self.api_secret, algorithm='RS256', headers=headers)

    def mute_all(
        self, session_id: str, excludedStreamIds: Optional[List[str]]
    ) -> requests.Response:
        """
        Mutes all streams in an OpenTok session.

        You can include an optional list of streams IDs to exclude from being muted.

        In addition to existing streams, any streams that are published after the call to
        this method are published with audio muted. You can remove the mute state of a session
        by calling the OpenTok.disable_force_mute() method.

        :param session_id The session ID

        :param excludedStreamIds A list of stream IDs for streams that should not be muted.
        This is an optional property. If you omit this property, all streams in the session will be muted.
        """

        options = {}
        url = self.endpoints.get_mute_all_url(session_id)

        try:
            if excludedStreamIds:
                options = {"active": True, "excludedStreams": excludedStreamIds}
            else:
                options = {"active": True, "excludedStreams": []}

            response = requests.post(
                url, headers=self.get_json_headers(), data=json.dumps(options)
            )

            if response:
                return response
            elif response.status_code == 400:
                raise GetStreamError(
                    "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
                )
            elif response.status_code == 403:
                raise AuthError("Failed to mute, invalid credentials.")
            elif response.status_code == 404:
                raise NotFoundError("The session or a stream is not found.")
        except Exception as e:
            raise OpenTokException(
                (
                    "There was an error thrown by the OpenTok SDK, please check that your session_id {0} and excludedStreamIds (if exists) {1} are valid"
                ).format(session_id, excludedStreamIds)
            )

    def disable_force_mute(self, session_id: str) -> requests.Response:
        """
        Disables the active mute state of the session. After you call this method, new streams
        published to the session will no longer have audio muted.

        After you call the mute_all() method, any streams published after
        the call are published with audio muted. Call the OpenTok.disable_force_mute() method
        to remove the mute state of a session, so that new published streams are not
        automatically muted.

        :param session_id The session ID.
        """

        options = {"active": False}
        url = self.endpoints.get_mute_all_url(session_id)

        response = requests.post(
            url, headers=self.get_json_headers(), data=json.dumps(options)
        )

        try:
            if response:
                return response
            elif response.status_code == 400:
                raise GetStreamError(
                    "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
                )
            elif response.status_code == 403:
                raise AuthError("Failed to mute, invalid credentials.")
            elif response.status_code == 404:
                raise NotFoundError("The session or a stream is not found.")
        except Exception as e:
            raise OpenTokException(
                (
                    "There was an error thrown by the OpenTok SDK, please check that your session_id {0} is valid"
                ).format(session_id)
            )

    def mute_stream(self, session_id: str, stream_id: str) -> requests.Response:
        """
        Mutes a single stream in an OpenTok session.

        :param session_id The session ID.

        :param stream_id The stream ID.
        """

        try:
            if stream_id:
                url = self.endpoints.get_stream_url(session_id, stream_id) + "/mute"

            response = requests.post(url, headers=self.get_json_headers())

            if response:
                return response
            elif response.status_code == 400:
                raise GetStreamError(
                    "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
                )
            elif response.status_code == 403:
                raise AuthError("Failed to mute, invalid credentials.")
            elif response.status_code == 404:
                raise NotFoundError("Mute not found")
        except Exception as e:
            raise OpenTokException(
                (
                    "There was an error thrown by the OpenTok SDK, please check that your session_id {0} and stream_id {1} are valid"
                ).format(session_id, stream_id)
            )

    def play_dtmf(
        self, session_id: str, connection_id: str, digits: str, options: dict = {}
    ) -> requests.Response:
        """
        Plays a DTMF string into a session or to a specific connection

        :param session_id The ID of the OpenTok session that the participant being called
        will join

        :param connection_id An optional parameter used to send the DTMF tones to a specific
        connection in a session.

        :param digits DTMF digits to play
        Valid DTMF digits are 0-9, p, #, and * digits. 'p' represents a 500ms pause if a delay is
        needed during the input process.

        """

        try:
            if not connection_id:
                url = self.endpoints.get_dtmf_all_url(session_id)
                payload = {"digits": digits}
            else:
                url = self.endpoints.get_dtmf_specific_url(session_id, connection_id)
                payload = {"digits": digits}

            response = requests.post(
                url, headers=self.get_json_headers(), data=json.dumps(payload)
            )

            if response.status_code == 200:
                return response
            elif response.status_code == 400:
                raise DTMFError(
                    "One of the properties digits, sessionId or connectionId is invalid."
                )
            elif response.status_code == 403:
                raise AuthError(
                    "Failed to create session, invalid credentials. Please check your OpenTok API Key or JSON web token"
                )
            elif response.status_code == 404:
                raise NotFoundError(
                    "The session does not exists or the client specified by the connection_id is not connected to the session"
                )
        except Exception as e:
            raise OpenTokException(
                (
                    f"There was an error thrown by the OpenTok SDK, please check that your session_id: {session_id}, connection_id (if exists): {connection_id} and digits: {digits} are valid"
                )
            )


class OpenTok(Client):
    def __init__(
        self,
        api_key,
        api_secret,
        api_url="https://api.opentok.com",
        timeout=None,
        app_version=None,
    ):
        warnings.warn(
            "OpenTok class is deprecated (Use Client class instead)",
            DeprecationWarning,
            stacklevel=2,
        )
        super(OpenTok, self).__init__(
            api_key,
            api_secret,
            api_url=api_url,
            timeout=timeout,
            app_version=app_version,
        )

    def mute_all(
        self, session_id: str, excludedStreamIds: Optional[List[str]]
    ) -> requests.Response:
        """
        Mutes all streams in an OpenTok session.
        You can include an optional list of streams IDs to exclude from being muted.
        In addition to existing streams, any streams that are published after the call to
        this method are published with audio muted. You can remove the mute state of a session
        by calling the OpenTok.disableForceMute() method.
        :param session_id The session ID
        :param excludedStreamIds A list of stream IDs for streams that should not be muted.
        This is an optional property. If you omit this property, all streams in the session will be muted.
        """

        options = {}
        url = self.endpoints.get_mute_all_url(session_id)

        try:
            if excludedStreamIds:
                options = {"active": True, "excludedStreams": excludedStreamIds}
            else:
                options = {"active": True, "excludedStreams": []}

            response = requests.post(
                url, headers=self.get_json_headers(), data=json.dumps(options)
            )

            if response:
                return response
            elif response.status_code == 400:
                raise GetStreamError(
                    "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
                )
            elif response.status_code == 403:
                raise AuthError("Failed to mute, invalid credentials.")
            elif response.status_code == 404:
                raise NotFoundError("The session or a stream is not found.")
        except Exception as e:
            raise OpenTokException(
                (
                    "There was an error thrown by the OpenTok SDK, please check that your session_id {0} and excludedStreamIds (if exists) {1} are valid"
                ).format(session_id, excludedStreamIds)
            )

    def disable_force_mute(self, session_id: str) -> requests.Response:
        """
        Disables the active mute state of the session. After you call this method, new streams
        published to the session will no longer have audio muted.
        After you call the mute_all() method, any streams published after
        the call are published with audio muted. Call the OpenTok.disable_force_mute() method
        to remove the mute state of a session, so that new published streams are not
        automatically muted.
        :param session_id The session ID.
        """

        options = {"active": False}
        url = self.endpoints.get_mute_all_url(session_id)

        response = requests.post(
            url, headers=self.get_json_headers(), data=json.dumps(options)
        )

        try:
            if response:
                return response
            elif response.status_code == 400:
                raise GetStreamError(
                    "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
                )
            elif response.status_code == 403:
                raise AuthError("Failed to mute, invalid credentials.")
            elif response.status_code == 404:
                raise NotFoundError("The session or a stream is not found.")
        except Exception as e:
            raise OpenTokException(
                (
                    "There was an error thrown by the OpenTok SDK, please check that your session_id {0} is valid"
                ).format(session_id)
            )

    def mute_stream(self, session_id: str, stream_id: str) -> requests.Response:
        """
        Mutes a single stream in an OpenTok session.
        :param session_id The session ID.
        :param stream_id The stream ID.
        """

        try:
            if stream_id:
                url = self.endpoints.get_stream_url(session_id, stream_id) + "/mute"

            response = requests.post(url, headers=self.get_json_headers())

            if response:
                return response
            elif response.status_code == 400:
                raise GetStreamError(
                    "Invalid request. This response may indicate that data in your request data is invalid JSON. Or it may indicate that you do not pass in a session ID or you passed in an invalid stream ID."
                )
            elif response.status_code == 403:
                raise AuthError("Failed to mute, invalid credentials.")
            elif response.status_code == 404:
                raise NotFoundError("Mute not found")
        except Exception as e:
            raise OpenTokException(
                (
                    "There was an error thrown by the OpenTok SDK, please check that your session_id {0} and stream_id {1} are valid"
                ).format(session_id, stream_id)
            )

    def play_dtmf(
        self, session_id: str, connection_id: str, digits: str, options: dict = {}
    ) -> requests.Response:
        """
        Plays a DTMF string into a session or to a specific connection
        :param session_id The ID of the OpenTok session that the participant being called
        will join
        :param connection_id An optional parameter used to send the DTMF tones to a specific
        connectoiin in a session.
        :param digits DTMF digits to play
        Valid DTMF digits are 0-9, p, #, and * digits. 'p' represents a 500ms pause if a delay is
        needed during the input process.
        """

        try:
            if not connection_id:
                url = self.endpoints.get_dtmf_all_url(session_id)
                payload = {"digits": digits}
            else:
                url = self.endpoints.get_dtmf_specific_url(session_id, connection_id)
                payload = {"digits": digits}

            response = requests.post(
                url, headers=self.get_json_headers(), data=json.dumps(payload)
            )

            if response.status_code == 200:
                return response
            elif response.status_code == 400:
                raise DTMFError(
                    "One of the properties digits, sessionId or connectionId is invalid."
                )
            elif response.status_code == 403:
                raise AuthError(
                    "Failed to create session, invalid credentials. Please check your OpenTok API Key or JSON web token"
                )
            elif response.status_code == 404:
                raise NotFoundError(
                    "The session does not exists or the client specified by the connection_id is not connected to the session"
                )
        except Exception as e:
            raise OpenTokException(
                (
                    f"There was an error thrown by the OpenTok SDK, please check that your session_id: {session_id}, connection_id (if exists): {connection_id} and digits: {digits} are valid"
                )
            )
