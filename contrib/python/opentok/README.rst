==================
OpenTok Python SDK
==================

.. image:: https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg 
   :target: CODE_OF_CONDUCT.md

.. image:: https://assets.tokbox.com/img/vonage/Vonage_VideoAPI_black.svg
   :height: 48px
   :alt: Tokbox is now known as Vonage

The OpenTok Python SDK lets you generate
`sessions <http://tokbox.com/opentok/tutorials/create-session/>`_ and
`tokens <http://tokbox.com/opentok/tutorials/create-token/>`_ for `OpenTok <http://www.tokbox.com/>`_
applications, and `archive <http://www.tokbox.com/platform/archiving>`_ OpenTok sessions.

Note!
-----

This library is designed to work with the Tokbox/OpenTok platform, part of the Vonage Video API. If you are looking to use the Vonage Video API and are using the Vonage Customer Dashboard, you will want to install the `Vonage Server SDK for Python <https://github.com/Vonage/vonage-python-sdk>`_, which includes support for the Vonage Video API.

Not sure which exact platform you are using? Take a look at `this guide <https://api.support.vonage.com/hc/en-us/articles/10817774782492>`_.

If you are using the Tokbox platform, do not worry! The Tokbox platform is not going away, and this library will continue to be updated. While we encourage customers to check out the new Unified platform, there is no rush to switch. Both platforms run the exact same infrastructure and capabilities. The main difference is a unified billing interface and easier access to `Vonage's other CPaaS APIs <https://www.vonage.com/communications-apis/>`_.

If you are new to the Vonage Video API, head on over to the `Vonage Customer Dashboard <https://dashboard.vonage.com>`_ to sign up for a developer account and check out the `Vonage Server SDK for Python <https://github.com/Vonage/vonage-python-sdk>`_. 


Installation using Pip (recommended):
-------------------------------------

Pip helps manage dependencies for Python projects using the PyPI index. Find more info here:
http://www.pip-installer.org/en/latest/

Add the ``opentok`` package as a dependency in your project. The most common way is to add it to your
``requirements.txt`` file::

  opentok>=3.0

Next, install the dependencies::

  $ pip install -r requirements.txt


Usage
-----

Initializing
~~~~~~~~~~~~

Import the package at the top of any file where you will use it. At the very least you will need the
``Client`` class. Then initialize a Client instance with your own API Key and API Secret.

.. code:: python

  from opentok import Client

  opentok = Client(api_key, api_secret)

Creating Sessions
~~~~~~~~~~~~~~~~~

To create an OpenTok Session, use the ``opentok.create_session()`` method. There are optional
keyword parameters for this method:

* ``location`` which can be set to a string containing an IP address.

* ``media_mode`` which is a String (defined by the MediaModes class).
  This determines whether the session will use the
  `OpenTok Media Router <https://tokbox.com/developer/guides/create-session/#media-mode>`_
  or attempt to send streams directly between clients. A routed session is required for some
  OpenTok features (such as archiving).

* ``archive_mode`` which specifies whether the session will be automatically archived (``always``)
  or not (``manual``).

* ``archive_name`` which indicates the archive name for all the archives in auto archived session. 
  A session that begins with archive mode 'always' will be using this archive name for all archives of that session.
  Passing 'archive_name' with archive mode 'manual' will cause an error response.

* ``archive_resolution`` which indicates the archive resolution for all the archives in auto archived session.
  Valid values are '640x480', '480x640', '1280x720', '720x1280', '1920x1080' and '1080x1920'.
  A session that begins with archive mode 'always' will be using this resolution for all archives of that session.
  Passing 'archive_resolution' with archive mode 'manual' will cause an error response.

* ``e2ee`` which is a boolean. This specifies whether to enable
  `end-to-end encryption <https://tokbox.com/developer/guides/end-to-end-encryption/>`_
  for the OpenTok session.

This method returns a ``Session`` object. Its ``session_id`` attribute is useful when saving to a persistent
store (such as a database).

.. code:: python

  # Create a session that attempts to send streams directly between clients (falling back
  # to use the OpenTok TURN server to relay streams if the clients cannot connect):
  session = opentok.create_session()

  from opentok import MediaModes
  # A session that uses the OpenTok Media Router, which is required for archiving:
  session = opentok.create_session(media_mode=MediaModes.routed)

  # An automatically archived session:
  session = opentok.create_session(media_mode=MediaModes.routed, archive_mode=ArchiveModes.always)

  # An automatically archived session with the archive name and resolution specified:
  session = opentok.create_session(
    media_mode=MediaModes.routed,
    archive_mode=ArchiveModes.always,
    archive_name='my_archive',
    archive_resolution='1920x1080'
  )

  # A session with a location hint
  session = opentok.create_session(location=u'12.34.56.78')

  # Store this session ID in the database
  session_id = session.session_id

Generating Tokens
~~~~~~~~~~~~~~~~~

Once a Session is created, you can start generating Tokens for clients to use when connecting to it.
You can generate a token either by calling the ``opentok.generate_token(session_id)`` method or by
calling the ``session.generate_token()`` method on a ``Session`` instance after creating it. Both
have a set of optional keyword parameters: ``role``, ``expire_time``, ``data``, and
``initial_layout_class_list``.

.. code:: python

  # Generate a Token from just a session_id (fetched from a database)
  token = opentok.generate_token(session_id)
  # Generate a Token by calling the method on the Session (returned from create_session)
  token = session.generate_token()

  from opentok import Roles
  # Set some options in a token
  token = session.generate_token(role=Roles.moderator,
                                 expire_time=int(time.time()) + 10,
                                 data=u'name=Johnny'
                                 initial_layout_class_list=[u'focus'])

Working with Archives
~~~~~~~~~~~~~~~~~~~~~

You can only archive sessions that use the OpenTok Media
Router (sessions with the media mode set to routed).

You can start the recording of an OpenTok Session using the ``opentok.start_archive(session_id)``
method. This method takes an optional keyword argument ``name`` to assign a name to the archive.
This method will return an ``Archive`` instance. Note that you can only start an Archive on
a Session that has clients connected.

.. code:: python

  archive = opentok.start_archive(session_id, name=u'Important Presentation')

  # Store this archive_id in the database
  archive_id = archive.id

You can also disable audio or video recording by setting the `has_audio` or `has_video` property of
the `options` parameter to `false`:

.. code:: python

  archive = opentok.start_archive(session_id, name=u'Important Presentation', has_video=False)

  # Store this archive_id in the database
  archive_id = archive.id

By default, all streams are recorded to a single (composed) file. You can record the different
streams in the session to individual files (instead of a single composed file) by setting the
``output_mode`` parameter of the ``opentok.start_archive()`` method `OutputModes.individual`.

.. code:: python

  archive = opentok.start_archive(session_id, name=u'Important Presentation', output_mode=OutputModes.individual)

  # Store this archive_id in the database
  archive_id = archive.id

To add an individual stream to an archive, use the
``opentok.add_archive_stream(archive_id, stream_id, has_audio, has_video)`` method:

.. code:: python

  opentok.add_archive_stream(archive.id, stream_id, has_audio=True, has_video=True)

To remove a stream from an archive, use the ``opentok.remove_archive_stream()`` method:

.. code:: python

  opentok.remove_archive_stream(archive.id, stream_id)

Composed archives (output_mode=OutputModes.composed) have an optional ``resolution`` parameter.
If no value is supplied, the archive will use the default resolution, "640x480".
You can set this to another resolution by setting the
``resolution`` parameter of the ``opentok.start_archive()`` method.

You can specify the following archive resolutions:

* "640x480" (SD landscape, default resolution)
* "480x640" (SD portrait)
* "1280x720" (HD landscape)
* "720x1280" (HD portrait)
* "1920x1080" (FHD landscape)
* "1080x1920" (FHD portrait)

Setting the ``resolution`` parameter while setting the ``output_mode`` parameter to
``OutputModes.individual`` results in an error.

.. code:: python

  archive = opentok.start_archive(session_id, name=u'Important Presentation', resolution="1280x720")

  # Store this archive_id in the database
  archive_id = archive.id

You can enable multiple simultaneous archives by specifying a unique value for the ``multi_archive_tag`` 
parameter in the ``start_archive`` method.

.. code:: python

  archive = opentok.start_archive(session_id, name=u'Important Presentation', multi_archive_tag='MyArchiveTag')

You can stop the recording of a started Archive using the ``opentok.stop_archive(archive_id)``method. 
You can also do this using the ``archive.stop()`` method of an ``Archive`` instance.

.. code:: python

  # Stop an Archive from an archive_id (fetched from database)
  opentok.stop_archive(archive_id)
  # Stop an Archive from an instance (returned from opentok.start_archive)
  archive.stop()

To get an ``Archive`` instance (and all the information about it) from an archive ID, use the
``opentok.get_archive(archive_id)`` method.

.. code:: python

  archive = opentok.get_archive(archive_id)

To delete an Archive, you can call the ``opentok.delete_archive(archive_id)`` method or the
``archive.delete()`` method of an ``Archive`` instance.

.. code:: python

  # Delete an Archive from an archive ID (fetched from database)
  opentok.delete_archive(archive_id)
  # Delete an Archive from an Archive instance (returned from opentok.start_archive or
  opentok.get_archive)
  archive.delete()

You can also get a list of all the Archives you've created (up to 1000) with your API Key. This is
done using the ``opentok.list_archives()`` method. There are three optional keyword parameters:
``count``, ``offset`` and ``session_id``; they can help you paginate through the results and
filter by session ID. This method returns an instance of the ``ArchiveList`` class.

.. code:: python

  archive_list = opentok.list_archives()

  # Get a specific Archive from the list
  archive = archive_list.items[i]

  # Iterate over items
  for archive in iter(archive_list):
    pass

  # Get the total number of Archives for this API Key
  total = archive_list.total

Note that you can also create an automatically archived session, by passing in
``ArchiveModes.always`` as the ``archive_mode`` parameter when you call the
``opentok.create_session()`` method (see "Creating Sessions," above).

For composed archives, you can change the layout dynamically, using the `opentok.set_archive_layout(archive_id, type, stylesheet)` method:

.. code:: python

  opentok.set_archive_layout('ARCHIVEID', 'horizontalPresentation')

Setting the layout of composed archives is optional. By default, composed archives use the `best fit` layout.  Other valid values are: `custom`, `horizontalPresentation`, `pip` and `verticalPresentation`. If you specify a `custom` layout type, set the stylesheet parameter:

.. code:: python

  opentok.set_archive_layout(
      'ARCHIVEID',
      'custom',
      'stream.instructor {position: absolute; width: 100%;  height:50%;}'
  )

For other layout types, do not set the stylesheet property. For more information see
`Customizing the video layout for composed archives <https://tokbox.com/developer/guides/archiving/layout-control.html>`_.

For more information on archiving, see the
`OpenTok archiving <https://tokbox.com/opentok/tutorials/archiving/>`_ programming guide.

Sending Signals
~~~~~~~~~~~~~~~~~~~~~

Once a Session is created, you can send signals to everyone in the session or to a specific connection. You can send a signal by calling the ``signal(session_id, payload)`` method of the ``Client`` class. The ``payload`` parameter is a dictionary used to set the ``type``, ``data`` fields. Ỳou can also call the method with the parameter ``connection_id`` to send a signal to a specific connection ``signal(session_id, data, connection_id)``.

.. code:: python

  # payload structure
  payload = {
      'type': 'type', #optional
      'data': 'signal data' #required
  }

  connection_id = '2a84cd30-3a33-917f-9150-49e454e01572'

  # To send a signal to everyone in the session:
  opentok.signal(session_id, payload)

  # To send a signal to a specific connection in the session:
  opentok.signal(session_id, payload, connection_id)

Working with Streams
~~~~~~~~~~~~~~~~~~~~~

You can get information about a stream by calling the `get_stream(session_id, stream_id)` method of the `Client` class.

The method returns a Stream object that contains information of an OpenTok stream:

* ``id``: The stream ID
* ``videoType``: "camera" or "screen"
* ``name``: The stream name (if one was set when the client published the stream)
* ``layoutClassList``: It's an array of the layout classes for the stream

.. code:: python

  session_id = 'SESSIONID'
  stream_id = '8b732909-0a06-46a2-8ea8-074e64d43422'

  # To get stream info:
  stream = opentok.get_stream(session_id, stream_id)

  # Stream properties:
  print stream.id #8b732909-0a06-46a2-8ea8-074e64d43422
  print stream.videoType #camera
  print stream.name #stream name
  print stream.layoutClassList #['full']

Also, you can get information about all the streams in a session by calling the `list_streams(session_id)` method of the `Client` class.

The method returns a StreamList object that contains a list of all the streams

.. code:: python

  # To get all streams in a session:
  stream_list = opentok.list_streams(session_id)

  # Getting the first stream of the list
  stream = stream_list.items[0]

  # Stream properties:
  print stream.id #8b732909-0a06-46a2-8ea8-074e64d43422
  print stream.videoType #camera
  print stream.name #stream name
  print stream.layoutClassList #['full']

You can change the layout classes for streams in a session by calling the `set_stream_class_lists(session_id, stream_list)` method of the `Client` class.

The layout classes define how the stream is displayed in the layout of a composed OpenTok archive.

.. code:: python

  # This list contains the information of the streams that will be updated. Each element
  # in the list is a dictionary with two properties: 'id' and 'layoutClassList'. The 'id'
  # property is the stream ID (a String), and the 'layoutClassList' is an array of class
  # names (Strings) to apply to the stream.
  payload = [
      {'id': '7b09ec3c-26f9-43d7-8197-f608f13d4fb6', 'layoutClassList': ['focus']},
      {'id': '567bc941-6ea0-4c69-97fc-70a740b68976', 'layoutClassList': ['top']},
      {'id': '307dc941-0450-4c09-975c-705740d08970', 'layoutClassList': ['bottom']}
  ]

  opentok.set_stream_class_lists('SESSIONID', payload)

For more information see
`Changing the composed archive layout classes for an OpenTok stream <https://tokbox.com/developer/rest/#change-stream-layout-classes-composed>`_.

Force Disconnect
~~~~~~~~~~~~~~~~~~~~~

Your application server can disconnect a client from an OpenTok session by calling the force_disconnect(session_id, connection_id) method of the Client class, or the force_disconnect(connection_id) method of the Session class.

.. code:: python

  session_id = 'SESSIONID'
  connection_id = 'CONNECTIONID'

  # To send a request to disconnect a client:
  opentok.force_disconnect(session_id, connection_id)

Working with SIP Interconnect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can connect your SIP platform to an OpenTok session, the audio from your end of the SIP call is added to the OpenTok session as an audio-only stream. 
The OpenTok Media Router mixes audio from other streams in the session and sends the mixed audio to your SIP endpoint.

.. code:: python

  session_id = u('SESSIONID')
  token = u('TOKEN')
  sip_uri = u('sip:user@sip.partner.com;transport=tls')

  # call the method with the required parameters
  sip_call = opentok.dial(session_id, token, sip_uri)

  # the method also supports aditional options to establish the sip call
  options = {
      'from': 'from@example.com',
      'headers': {
          'headerKey': 'headerValue'
      },
      'auth': {
          'username': 'username',
          'password': 'password'
      },
      'secure': True,
      'video': True,
      'observeForceMute': True,
      'streams': ['stream-id-1', 'stream-id-2']
  }

  # call the method with aditional options
  sip_call = opentok.dial(session_id, token, sip_uri, options)

For more information, including technical details and security considerations, see the
`OpenTok SIP interconnect <https://tokbox.com/developer/guides/sip/>`_ developer guide.

Working with Broadcasts
~~~~~~~~~~~~~~~~~~~~~~~

OpenTok broadcast lets you share live OpenTok sessions with many viewers.

You can use the ``opentok.start_broadcast()`` method to start a live stream for an OpenTok session.
This broadcasts the session to HLS (HTTP live streaming) or to RTMP streams.

To successfully start broadcasting a session, at least one client must be connected to the session.

The live streaming broadcast can target one HLS endpoint and up to five RTMP servers simulteneously for a session. You can only start live streaming for sessions that use the OpenTok Media Router; you cannot use live streaming with sessions that have the media mode set to relayed.

.. code:: python

  session_id = 'SESSIONID'
  options = {
    'layout': {
      'type': 'custom',
      'stylesheet': 'the layout stylesheet (only used with type == custom)'
    },
    'maxDuration': 5400,
    'hasAudio': True,
    'hasVideo': True,
    'maxBitrate': 2000000,
    'outputs': {
      'hls': {},
      'rtmp': [{
        'id': 'foo',
        'serverUrl': 'rtmp://myfooserver/myfooapp',
        'streamName': 'myfoostream'
      }, {
        'id': 'bar',
        'serverUrl': 'rtmp://mybarserver/mybarapp',
        'streamName': 'mybarstream'
      }]
    },
    'resolution': '640x480'
  }

  broadcast = opentok.start_broadcast(session_id, options)

You can specify the following broadcast resolutions:

* "640x480" (SD landscape, default resolution)
* "480x640" (SD portrait)
* "1280x720" (HD landscape)
* "720x1280" (HD portrait)
* "1920x1080" (FHD landscape)
* "1080x1920" (FHD portrait)

You can specify a maximum bitrate between 100000 and 6000000.

.. code:: python

  session_id = 'SESSIONID'
  options = {
    'multiBroadcastTag': 'unique_broadcast_tag'
    'layout': {
      'type': 'custom',
      'stylesheet': 'the layout stylesheet (only used with type == custom)'
    },
    'maxDuration': 5400,
    'maxBitrate': 2000000,
    'outputs': {
      'hls': {},
      'rtmp': [{
        'id': 'foo',
        'serverUrl': 'rtmp://myfooserver/myfooapp',
        'streamName': 'myfoostream'
      }, {
        'id': 'bar',
        'serverUrl': 'rtmp://mybarserver/mybarapp',
        'streamName': 'mybarstream'
      }]
    },
    'resolution': '640x480'
  }

  broadcast = opentok.start_broadcast(session_id, options)
  
To enable multiple simultaneous broadcasts on the same session, specify a unique value for the 
``multiBroadcastTag`` parameter in ``options`` when calling the ``opentok.start_broadcast`` method.

You can broadcast only audio, or only video, for a stream by setting ``hasAudio`` or ``hasVideo``
to ``False`` as required. These fields are ``True`` by default.

.. code:: python

  session_id = 'SESSIONID'
  options = {
    'layout': {
      'type': 'custom',
      'stylesheet': 'the layout stylesheet (only used with type == custom)'
    },
    'maxDuration': 5400,
    'hasAudio': True,
    'hasVideo': False,
    'maxBitrate': 2000000,
    'outputs': {
      'hls': {},
      'rtmp': [{
        'id': 'foo',
        'serverUrl': 'rtmp://myfooserver/myfooapp',
        'streamName': 'myfoostream'
      }, {
        'id': 'bar',
        'serverUrl': 'rtmp://mybarserver/mybarapp',
        'streamName': 'mybarstream'
      }]
    },
    'resolution': '640x480'
  }

  broadcast = opentok.start_broadcast(session_id, options)

You can stop a started Broadcast using the ``opentok.stop_broadcast(broadcast_id)`` method.

.. code:: python

  # getting the ID from a broadcast object
  broadcast_id = broadcast.id

  # stop a broadcast
  broadcast = opentok.stop_broadcast(broadcast_id)

You can get details on a broadcast that is in-progress using the method ``opentok.get_broadcast(broadcast_id)``.

.. code:: python

  broadcast_id = '1748b7070a81464c9759c46ad10d3734'

  # get broadcast details
  broadcast = opentok.get_broadcast(broadcast_id)

  print broadcast.json()

  # print result
  # {
  #   "createdAt": 1437676551000,
  #   "id": "1748b707-0a81-464c-9759-c46ad10d3734",
  #   "projectId": 100,
  #   "resolution": "640x480",
  #   "sessionId": "2_MX4xMDBfjE0Mzc2NzY1NDgwMTJ-TjMzfn4",
  #   "status": "started",
  #   "updatedAt": 1437676551000,
  #   "broadcastUrls": {
  #       "hls": "http://server/fakepath/playlist.m3u8",
  #       "rtmp": {
  #           "bar": {
  #               "serverUrl": "rtmp://mybarserver/mybarapp",
  #               "status": "live",
  #               "streamName": "mybarstream"
  #           },
  #           "foo": {
  #               "serverUrl": "rtmp://myfooserver/myfooapp",
  #               "status": "live",
  #               "streamName": "myfoostream"
  #           }
  #       }
  #   }
  # }

You can dynamically change the layout type of a live streaming broadcast.

.. code:: python

  # Valid values to 'layout_type' are: 'custom', 'horizontalPresentation',
  # 'pip' and 'verticalPresentation' 
  opentok.set_broadcast_layout('BROADCASTID', 'horizontalPresentation')

  # if you specify a 'custom' layout type, set the stylesheet parameter:
  opentok.set_broadcast_layout(
      'BROADCASTID',
      'custom',
      'stream.instructor {position: absolute; width: 100%;  height:50%;}'
  )

You can add streams to a broadcast using the ``opentok.add_broadcast_stream()`` method:

.. code:: python

  opentok.add_broadcast_stream(broadcast_id, stream_id)

Conversely, streams can be removed from a broadcast with the ``opentok.remove_broadcast_stream()`` method.

.. code:: python

  opentok.remove_broadcast_stream(broadcast_id, stream_id)

For more information about OpenTok live streaming broadcasts, see the
`Broadcast developer guide <https://tokbox.com/developer/guides/broadcast/>`_.


Connecting audio to a WebSocket
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can send audio to a WebSocket with the ``opentok.connect_audio_to_websocket`` method.
For more information, see the
`Audio Connector developer guide <https://tokbox.com/developer/guides/audio-connector/>`_.

.. code:: python

  websocket_options = {"uri": "wss://service.com/ws-endpoint"}
  websocket_audio_connection = opentok.connect_audio_to_websocket(session_id, opentok_token, websocket_options)

Additionally, you can list only the specific streams you want to send to the WebSocket, and/or the additional headers that are sent, 
by adding these fields to the ``websocket_options`` object.

.. code:: python

  websocket_options = {
    "uri": "wss://service.com/ws-endpoint",
    "streams": [
      "streamId-1",
      "streamId-2"
    ],
    "headers": {
      "headerKey": "headerValue"
    }
  }


Using the Live Captions API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can enable live captioning for an OpenTok session with the ``opentok.start_captions`` method.
For more information, see the
`Live Captions API developer guide <https://tokbox.com/developer/guides/live-captions/>`.

.. code:: python

  captions = opentok.start_captions(session_id, opentok_token)

You can also specify optional parameters, as shown below.

.. code:: python

  captions = opentok.start_captions(
    session_id,
    opentok_token,
    language_code='en-GB',
    max_duration=10000,
    partial_captions=False,
    status_callback_url='https://example.com',
  )

You can stop an ongoing live captioning session by calling the ``opentok.stop_captions`` method.

.. code:: python

  opentok.stop_captions(captions_id)

Configuring Timeout
-------------------
Timeout is passed in the Client constructor:

``self.timeout = timeout``

In order to configure timeout, first create an instance:

``opentok = Client(...., timeout=value)``

And then proceed to change the value with

``opentok.timeout = value``

Muting streams
--------------

You can mute all streams in a session using the ``opentok.mute_all()`` method:

.. code:: python

  opentok.mute_all(session_id)

  # You can also specify streams to exclude (e.g. main presenter)
  excluded_stream_ids = ['1234', '5678']
  opentok.mute_all(session_id, excluded_stream_ids)

In addition to existing streams, any streams that are published after the call to
this method are published with audio muted. You can remove the mute state of a session
by calling the ``opentok.disableForceMute()`` method:

.. code:: python

  opentok.disable_force_mute(session_id)

After calling the ``opentok.disableForceMute()`` method, new streams that are published
to the session will not be muted.

You can mute a single stream using the ``opentok.mute_stream()`` method:

.. code:: python

  opentok.mute_stream(session_id, stream_id)

DTMF
------

You can send dual-tone multi-frequency (DTMF) digits to SIP endpoints. You can play DTMF tones
to all clients connected to session or to a specific connection:

.. code:: python
  
  digits = '12345'
  opentok.play_dtmf(session_id, digits)

  # To a specific connection
  opentok.play_dtmf(session_id, connection_id, digits)

Appending to the User Agent
---------------------------

You can append a string to the user agent that is sent with requests:

.. code:: python

  opentok.append_to_user_agent('my-appended-string')

Samples
-------

There are two sample applications included in this repository. To get going as fast as possible, clone the whole
repository and follow the Walkthroughs:

- `HelloWorld <sample/HelloWorld/README.md>`_
- `Archiving <sample/Archiving/README.md>`_

Documentation
-------------

Reference documentation is available at https://tokbox.com/developer/sdks/python/reference/.

Requirements
------------

You need an OpenTok API key and API secret, which you can obtain at https://dashboard.tokbox.com/

The OpenTok Python SDK requires Python 3.5 or higher

Release Notes
-------------

See the `Releases <https://github.com/opentok/Opentok-Python-SDK/releases>`_ page for details about
each release.

Important changes since v2.2
----------------------------

**Changes in v2.2.1:**

The default setting for the create_session() method is to create a session with the media mode set
to relayed. In previous versions of the SDK, the default setting was to use the OpenTok Media Router
(media mode set to routed). In a relayed session, clients will attempt to send streams directly
between each other (peer-to-peer); if clients cannot connect due to firewall restrictions, the
session uses the OpenTok TURN server to relay audio-video streams.

**Changes in v2.2.0:**

This version of the SDK includes support for working with OpenTok archives.

The Client.create_session() method now includes a media_mode parameter, instead of a p2p parameter.

**Changes in v3.X.X:**

This version of the SDK includes significant improvements such as top level entity naming, where the Opentok class is now `Client`.  We also implemented a standardised logging module, improved naming conventions and JWT generation to make developer experience more rewarding.

For details, see the reference documentation at
https://tokbox.com/developer/sdks/python/reference/.

Development and Contributing
----------------------------

Interested in contributing? We :heart: pull requests! See the `Development <DEVELOPING.md>`_ and
`Contribution <CONTRIBUTING.md>`_ guidelines.

Getting Help
------------

We love to hear from you so if you have questions, comments or find a bug in the project, let us know! You can either:

* Open an issue on this repository
* See https://support.tokbox.com/ for support options
* Tweet at us! We're `@VonageDev on Twitter <https://twitter.com/VonageDev>`_
* Or `join the Vonage Developer Community Slack <https://developer.nexmo.com/community/slack>`_
