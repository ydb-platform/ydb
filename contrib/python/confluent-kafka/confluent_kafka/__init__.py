#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from .deserializing_consumer import DeserializingConsumer
from .serializing_producer import SerializingProducer
from .error import KafkaException, KafkaError
from ._model import (Node,  # noqa: F401
                     ConsumerGroupTopicPartitions,
                     ConsumerGroupState,
                     ConsumerGroupType,
                     TopicCollection,
                     TopicPartitionInfo,
                     IsolationLevel,
                     ElectionType)

from .cimpl import (Producer,
                    Consumer,
                    Message,
                    TopicPartition,
                    Uuid,
                    libversion,
                    version,
                    TIMESTAMP_NOT_AVAILABLE,
                    TIMESTAMP_CREATE_TIME,
                    TIMESTAMP_LOG_APPEND_TIME,
                    OFFSET_BEGINNING,
                    OFFSET_END,
                    OFFSET_STORED,
                    OFFSET_INVALID)

__all__ = ['admin', 'Consumer',
           'KafkaError', 'KafkaException',
           'kafkatest', 'libversion', 'Message',
           'OFFSET_BEGINNING', 'OFFSET_END', 'OFFSET_INVALID', 'OFFSET_STORED',
           'Producer', 'DeserializingConsumer',
           'SerializingProducer', 'TIMESTAMP_CREATE_TIME', 'TIMESTAMP_LOG_APPEND_TIME',
           'TIMESTAMP_NOT_AVAILABLE', 'TopicPartition', 'Node',
           'ConsumerGroupTopicPartitions', 'ConsumerGroupState',
           'ConsumerGroupType', 'Uuid',
           'IsolationLevel', 'TopicCollection', 'TopicPartitionInfo']

__version__ = version()[0]


class ThrottleEvent(object):
    """
    ThrottleEvent contains details about a throttled request.
    Set up a throttle callback by setting the ``throttle_cb`` configuration
    property to a callable that takes a ThrottleEvent object as its only argument.
    The callback will be triggered from poll(), consume() or flush() when a request
    has been throttled by the broker.

    This class is typically not user instantiated.

    :ivar str broker_name: The hostname of the broker which throttled the request
    :ivar int broker_id: The broker id
    :ivar float throttle_time: The amount of time (in seconds) the broker throttled (delayed) the request
    """

    def __init__(self, broker_name,
                 broker_id,
                 throttle_time):
        self.broker_name = broker_name
        self.broker_id = broker_id
        self.throttle_time = throttle_time

    def __str__(self):
        return "{}/{} throttled for {} ms".format(self.broker_name, self.broker_id,
                                                  int(self.throttle_time * 1000))


def _resolve_plugins(plugins):
    """ Resolve embedded plugins from the wheel's library directory.

        For internal module use only.

        :param str plugins: The plugin.library.paths value
    """
    import os
    from sys import platform

    # Location of __init__.py and the embedded library directory
    basedir = os.path.dirname(__file__)

    if platform in ('win32', 'cygwin'):
        paths_sep = ';'
        ext = '.dll'
        libdir = basedir
    elif platform in ('linux', 'linux2'):
        paths_sep = ':'
        ext = '.so'
        libdir = os.path.join(basedir, '.libs')
    elif platform == 'darwin':
        paths_sep = ':'
        ext = '.dylib'
        libdir = os.path.join(basedir, '.dylibs')
    else:
        # Unknown platform, there are probably no embedded plugins.
        return plugins

    if not os.path.isdir(libdir):
        # No embedded library directory, probably not a wheel installation.
        return plugins

    resolved = []
    for plugin in plugins.split(paths_sep):
        if '/' in plugin or '\\' in plugin:
            # Path specified, leave unchanged
            resolved.append(plugin)
            continue

        # See if the plugin can be found in the wheel's
        # embedded library directory.
        # The user might not have supplied a file extension, so try both.
        good = None
        for file in [plugin, plugin + ext]:
            fpath = os.path.join(libdir, file)
            if os.path.isfile(fpath):
                good = fpath
                break

        if good is not None:
            resolved.append(good)
        else:
            resolved.append(plugin)

    return paths_sep.join(resolved)
