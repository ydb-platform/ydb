python-slackclient
===================

A client for Slack, which supports the Slack Web API and Real Time Messaging (RTM) API.

|build-status| |windows-build-status| |codecov| |doc-status| |pypi-version| |python-version|

.. |build-status| image:: https://travis-ci.org/slackapi/python-slackclient.svg?branch=master
    :target: https://travis-ci.org/slackapi/python-slackclient
.. |windows-build-status| image:: https://ci.appveyor.com/api/projects/status/rif04t60ptslj32x/branch/master?svg=true
    :target: https://ci.appveyor.com/project/slackapi/python-slackclient
.. |codecov| image:: https://codecov.io/gh/slackapi/python-slackclient/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/slackapi/python-slackclient
.. |doc-status| image:: https://readthedocs.org/projects/python-slackclient/badge/?version=latest
    :target: http://python-slackclient.readthedocs.io/en/latest/?badge=latest
.. |pypi-version| image:: https://badge.fury.io/py/slackclient.svg
    :target: https://pypi.python.org/pypi/slackclient
.. |python-version| image:: https://img.shields.io/pypi/pyversions/slackclient.svg
    :target: https://pypi.python.org/pypi/slackclient

Overview
--------

Whether you're building a custom app for your team, or integrating a third party
service into your Slack workflows, Slack Developer Kit for Python allows you to leverage the flexibility
of Python to get your project up and running as quickly as possible.

Documentation
***************

For comprehensive method information and usage examples, see the `full documentation <http://slackapi.github.io/python-slackclient>`_.

If you're building a project to receive content and events from Slack, check out the `Python Slack Events API Adapter <https://github.com/slackapi/python-slack-events-api/>`_ library.

You may also review our `Development Roadmap <https://github.com/slackapi/python-slackclient/wiki/Slack-Python-SDK-Roadmap>`_ in the project wiki.


Requirements and Installation
******************************

We recommend using `PyPI <https://pypi.python.org/pypi>`_ to install Slack Developer Kit for Python

.. code-block:: bash

	pip install slackclient

Of course, if you prefer doing things the hard way, you can always implement Slack Developer Kit for Python
by pulling down the source code directly into your project:

.. code-block:: bash

	git clone https://github.com/slackapi/python-slackclient.git
	pip install -r requirements.txt

Getting Help
*************

If you get stuck, we’re here to help. The following are the best ways to get assistance working through your issue:

- Use our `Github Issue Tracker <https://github.com/slackapi/python-slackclient/issues>`_ for reporting bugs or requesting features.
- Visit the `Bot Developer Hangout <http://community.botkit.ai>`_ for getting help using Slack Developer Kit for Python or just generally bond with your fellow Slack developers.

Basic Usage
------------
The Slack Web API allows you to build applications that interact with Slack in more complex ways than the integrations
we provide out of the box.

This package is a modular wrapper designed to make Slack `Web API <https://api.slack.com/web>`_ calls simpler and easier for your
app. Provided below are examples of how to interact with commonly used API endpoints, but this is by no means
a complete list. Review the full list of available methods `here <https://api.slack.com/methods>`_.


Sending a message
********************
The primary use of Slack is sending messages. Whether you're sending a message
to a user or to a channel, this method handles both.

To send a message to a channel, use the channel's ID. For IMs, use the user's ID.

.. code-block:: python

  import os
  from slackclient import SlackClient

  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)

  sc.api_call(
    "chat.postMessage",
    channel="C0XXXXXX",
    text="Hello from Python! :tada:"
  )

There are some unique options specific to sending IMs, so be sure to read the **channels**
section of the `chat.postMessage <https://api.slack.com/methods/chat.postMessage#channels>`_
page for a full list of formatting and authorship options.

Sending an ephemeral message, which is only visible to an assigned user in a specified channel, is nearly the same
as sending a regular message, but with an additional ``user`` parameter.

.. code-block:: python

  import os
  from slackclient import SlackClient

  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)

  sc.api_call(
    "chat.postEphemeral",
    channel="C0XXXXXX",
    text="Hello from Python! :tada:",
    user="U0XXXXXXX"
  )

See `chat.postEphemeral <https://api.slack.com/methods/chat.postEphemeral>`_ for more info.


Replying to messages and creating threads
*****************************************
Threaded messages are just like regular messages, except thread replies are grouped together to provide greater context
to the user. You can reply to a thread or start a new threaded conversation by simply passing the original message's ``ts``
ID in the ``thread_ts`` attribute when posting a message. If you're replying to a threaded message, you'll pass the `thread_ts`
ID of the message you're replying to.

A channel or DM conversation is a nearly linear timeline of messages exchanged between people, bots, and apps.
When one of these messages is replied to, it becomes the parent of a thread. By default, threaded replies do not
appear directly in the channel, instead relegated to a kind of forked timeline descending from the parent message.

.. code-block:: python

  import os
  from slackclient import SlackClient

  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)

  sc.api_call(
    "chat.postMessage",
    channel="C0XXXXXX",
    text="Hello from Python! :tada:",
    thread_ts="1476746830.000003"
  )


By default, ``reply_broadcast`` is set to ``False``. To indicate your reply is germane to all members of a channel,
set the ``reply_broadcast`` boolean parameter to ``True``.

.. code-block:: python

  import os
  from slackclient import SlackClient

  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)

  sc.api_call(
    "chat.postMessage",
    channel="C0XXXXXX",
    text="Hello from Python! :tada:",
    thread_ts="1476746830.000003",
    reply_broadcast=True
  )


**Note:** While threaded messages may contain attachments and message buttons, when your reply is broadcast to the
channel, it'll actually be a reference to your reply, not the reply itself.
So, when appearing in the channel, it won't contain any attachments or message buttons. Also note that updates and
deletion of threaded replies works the same as regular messages.

See the `Threading messages together <https://api.slack.com/docs/message-threading#forking_conversations>`_
article for more information.


Deleting a message
********************
Sometimes you need to delete things.

.. code-block:: python

  import os
  from slackclient import SlackClient

  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)

  sc.api_call(
    "chat.delete",
    channel="C0XXXXXX",
    ts="1476745373.000002"
  )

See `chat.delete <https://api.slack.com/methods/chat.delete>`_ for more info.

Adding or removing an emoji reaction
****************************************
You can quickly respond to any message on Slack with an emoji reaction. Reactions
can be used for any purpose: voting, checking off to-do items, showing excitement — and just for fun.

This method adds a reaction (emoji) to an item (``file``, ``file comment``, ``channel message``, ``group message``, or ``direct message``). One of file, file_comment, or the combination of channel and timestamp must be specified.

.. code-block:: python

  import os
  from slackclient import SlackClient

  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)

  sc.api_call(
    "reactions.add",
    channel="C0XXXXXXX",
    name="thumbsup",
    timestamp="1234567890.123456"
  )

Removing an emoji reaction is basically the same format, but you'll use ``reactions.remove`` instead of ``reactions.add``

.. code-block:: python

  sc.api_call(
    "reactions.remove",
    channel="C0XXXXXXX",
    name="thumbsup",
    timestamp="1234567890.123456"
  )


See `reactions.add <https://api.slack.com/methods/reactions.add>`_ and `reactions.remove <https://api.slack.com/methods/reactions.remove>`_ for more info.

Getting a list of channels
******************************
At some point, you'll want to find out what channels are available to your app. This is how you get that list.

**Note:** This call requires the ``channels:read`` scope.

.. code-block:: python

  sc.api_call("channels.list")

Archived channels are included by default. You can exclude them by passing ``exclude_archived=1`` to your request.

.. code-block:: python

  sc.api_call(
    "channels.list",
    exclude_archived=1
  )

See `channels.list <https://api.slack.com/methods/channels.list>`_ for more info.

Getting a channel's info
*************************
Once you have the ID for a specific channel, you can fetch information about that channel.

.. code-block:: python

  sc.api_call(
    "channels.info",
    channel="C0XXXXXXX"
  )

See `channels.info <https://api.slack.com/methods/channels.info>`_ for more info.

Joining a channel
********************
Channels are the social hub of most Slack teams. Here's how you hop into one:

.. code-block:: python

  sc.api_call(
    "channels.join",
    channel="C0XXXXXXY"
  )

If you are already in the channel, the response is slightly different.
``already_in_channel`` will be true, and a limited ``channel`` object will be returned. Bot users cannot join a channel on their own, they need to be invited by another user.

See `channels.join <https://api.slack.com/methods/channels.join>`_ for more info.

Leaving a channel
********************
Maybe you've finished up all the business you had in a channel, or maybe you
joined one by accident. This is how you leave a channel.

.. code-block:: python

  sc.api_call(
    "channels.leave",
    channel="C0XXXXXXX"
  )

See `channels.leave <https://api.slack.com/methods/channels.leave>`_ for more info.


Tokens and Authentication
**************************

The simplest way to create an instance of the client, as shown in the samples above, is to use a bot (xoxb) access token:

.. code-block:: python

  # Get the access token from environmental variable
  slack_token = os.environ["SLACK_API_TOKEN"]
  sc = SlackClient(slack_token)


The SlackClient library allows you to use a variety of Slack authentication tokens.

To take advantage of automatic token refresh, you'll need to instantiate the client a little differently than when using
a bot access token. With a bot token, you have the access (xoxb) token when you create the client, when using refresh tokens,
you won't know the access token when the client is created.

Upon the first request, the SlackClient will request a new access (xoxa) token on behalf of your application, using your app's
refresh token, client ID, and client secret.

.. code-block:: python

    # Get the access token from environmental variable
    slack_refresh_token = os.environ["SLACK_REFRESH_TOKEN"]
    slack_client_id = os.environ["SLACK_CLIENT_ID"]
    slack_client_secret = os.environ["SLACK_CLIENT_SECRET"]


Since your app's access tokens will be expiring and refreshed, the client requires a callback method to be passed in on creation of the client.
Once Slack returns an access token for your app, the SlackClient will call your provided callback to update the access token in your datastore.

.. code-block:: python

    # This is where you'll add your data store update logic
    def token_update_callback(update_data):
        print("Enterprise ID: {}".format(update_data["enterprise_id"]))
        print("Workspace ID: {}".format(update_data["team_id"]))
        print("Access Token: {}".format(update_data["access_token"]))
        print("Access Token expires in (ms): {}".format(update_data["expires_in"]))

    # When creating an instance of the client, pass the client details and token update callback
    sc = SlackClient(
      refresh_token=slack_refresh_token,
      client_id=slack_client_id,
      client_secret=slack_client_secret,
      token_update_callback=token_update_callback
    )


Slack will send your callback function the **app's access token**, **token expiration TTL**, **team ID**, and **enterprise ID** (for enterprise workspaces)


See `Tokens & Authentication <http://slackapi.github.io/python-slackclient/auth.html#handling-tokens>`_ for API token handling best practices.



Additional Information
********************************************************************************************
For comprehensive method information and usage examples, see the `full documentation`_.
