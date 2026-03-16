# Copyright 2018 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging

import sockjs.tornado

from openhtf.util import classproperty

_LOG = logging.getLogger(__name__)


class PubSub(sockjs.tornado.SockJSConnection):
  """Generic pub/sub based on SockJS connections."""

  @classproperty
  def _lock(cls):  # pylint: disable=no-self-argument
    """Ensure subclasses don't share subscriber locks by forcing override."""
    raise AttributeError(
        'The PubSub class should not be instantiated directly. '
        'Instead, subclass it and override the _lock attribute.')

  @classproperty
  def subscribers(cls):  # pylint: disable=no-self-argument
    """Ensure subclasses don't share subscribers by forcing override."""
    raise AttributeError(
        'The PubSub class should not be instantiated directly. '
        'Instead, subclass it and override the subscribers attribute.')

  @classmethod
  def publish(cls, message, client_filter=None):
    """Publish messages to subscribers.

    Args:
      message: The message to publish.
      client_filter: A filter function to call passing in each client. Only
                     clients for whom the function returns True will have the
                     message sent to them.
    """
    with cls._lock:
      for client in cls.subscribers:
        if (not client_filter) or client_filter(client):
          client.send(message)

  def on_open(self, info):
    _LOG.debug('New subscriber from %s.', info.ip)
    with self._lock:
      self.subscribers.add(self)
    self.on_subscribe(info)

  def on_close(self):
    _LOG.debug('A client unsubscribed.')
    with self._lock:
      self.subscribers.remove(self)
    self.on_unsubscribe()

  def on_subscribe(self, info):
    """Called when new clients subscribe. Subclasses can override."""
    pass

  def on_unsubscribe(self):
    """Called when clients unsubscribe. Subclasses can override."""
    pass
