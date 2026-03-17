# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import contextlib
import itertools
import os
import sys

import six

from zake import utils

from kazoo import exceptions as k_exceptions
from kazoo.protocol import states as k_states


# See: https://issues.apache.org/jira/browse/ZOOKEEPER-243
SEQ_ROLLOVER = 2147483647
SEQ_ROLLOVER_TO = -2147483647
ROOT_PATH = '/'


class FakeStorage(object):
    """A place too place fake zookeeper paths + data + connected clients."""

    def __init__(self, handler, paths=None, sequences=None):
        if paths:
            self._paths = dict(paths)
        else:
            self._paths = {}
        if sequences:
            self._sequences = dict(sequences)
        else:
            self._sequences = {}
        self._lock = handler.rlock_object()
        # Ensure the root path *always* exists.
        if ROOT_PATH not in self._paths:
            self._paths[ROOT_PATH] = {
                'created_on': 0,
                'updated_on': 0,
                'version': 0,
                # Not supported for now...
                'aversion': -1,
                'cversion': -1,
                'data': b"",
                'ephemeral': False,
            }
        self._clients = set()
        self._client_lock = handler.rlock_object()

    @property
    def lock(self):
        return self._lock

    def attach(self, client):
        with self._client_lock:
            if client not in self._clients:
                self._clients.add(client)

    def _make_znode(self, node, child_count):
        # Not exactly right, but good enough...
        translated = {
            'czxid': node['version'],
            'mzxid': node['version'],
            'pzxid': node['version'],
            'ctime': node['created_on'],
            'mtime': node['updated_on'],
            'version': node['version'],
            'aversion': node['aversion'],
            'cversion': node['cversion'],
            'dataLength': len(node['data']),
            'numChildren': int(child_count),
        }
        if node['ephemeral']:
            translated['ephemeralOwner'] = node['ephemeral_owner']
        else:
            translated['ephemeralOwner'] = 0
        return k_states.ZnodeStat(**translated)

    @property
    def paths(self):
        return self._paths

    @property
    def sequences(self):
        return self._sequences

    @property
    def clients(self):
        return self._clients

    def __getitem__(self, path):
        return self._paths[path]

    def __setitem__(self, path, value):
        with self.lock:
            self._paths[path] = value

    def set(self, path, value, version=-1):
        with self.lock:
            if version != -1:
                stat = self.get(path)[1]
                if stat.version != version:
                    raise k_exceptions.BadVersionError("Version mismatch %s "
                                                       "!= %s" % (stat.version,
                                                                  version))
                self._paths[path]['data'] = value
                self._paths[path]['updated_on'] = utils.millitime()
                self._paths[path]['version'] += 1
            else:
                self._paths[path]['data'] = value
                self._paths[path]['updated_on'] = utils.millitime()
                self._paths[path]['version'] += 1
            return self.get(path)[1]

    def purge(self, client):
        if not client.session_id:
            return 0
        with self._client_lock:
            if client in self._clients:
                self._clients.discard(client)
            else:
                return 0
        removals = []
        with self.lock:
            for path, data in six.iteritems(self._paths):
                if data['ephemeral'] \
                   and data['ephemeral_owner'] == client.session_id:
                    removals.append(path)
            data_watches = []
            for path in removals:
                event = k_states.WatchedEvent(
                    type=k_states.EventType.DELETED,
                    state=k_states.KeeperState.CONNECTED,
                    path=path)
                data_watches.append(([path], event))
            fire_paths = []
            for path in removals:
                parents = sorted(six.iterkeys(self.get_parents(path)))
                for parent_path in parents:
                    if parent_path in fire_paths:
                        continue
                    fire_paths.append(parent_path)
            child_watches = []
            for path in fire_paths:
                event = k_states.WatchedEvent(
                    type=k_states.EventType.DELETED,
                    state=k_states.KeeperState.CONNECTED,
                    path=path)
                child_watches.append(([path], event))
            for path in removals:
                del self._paths[path]
        self.inform(client, child_watches, data_watches, inform_self=False)
        return len(removals)

    def inform(self, client, child_watches, data_watches, inform_self=True):
        with self._client_lock:
            clients = self._clients.copy()
        for other_client in clients:
            if not inform_self and other_client is client:
                continue
            other_client.fire_child_watches(child_watches)
            other_client.fire_data_watches(data_watches)

    def create(self, path, value=b"", sequence=False,
               ephemeral=False, session_id=None):

        def sequence_iter(path, parent_path):
            for i in itertools.count(0):
                sequence_id = self._sequences.get(parent_path, 0)
                if sequence_id == SEQ_ROLLOVER:
                    self._sequences[parent_path] = SEQ_ROLLOVER_TO
                else:
                    self._sequences[parent_path] = sequence_id + 1
                yield path + '%010d' % (sequence_id)

        parent_path, _node_name = os.path.split(path)
        with self.lock:
            if sequence:
                for possible_path in sequence_iter(path, parent_path):
                    if possible_path not in self:
                        path = possible_path
                        break
            parents = sorted(six.iterkeys(self.get_parents(path)))
            if parent_path not in self:
                if sequence:
                    self._sequences.pop(parent_path, None)
                raise k_exceptions.NoNodeError("Parent node %s does not exist"
                                               % (parent_path))
            if ephemeral and not session_id:
                raise k_exceptions.SystemZookeeperError("Ephemeral node %s can"
                                                        " not be created"
                                                        " without a session"
                                                        " id" % path)
            if path in self:
                raise k_exceptions.NodeExistsError("Node %s already"
                                                   " exists" % (path))
            for parent_path in reversed(parents):
                if self._paths[parent_path]['ephemeral']:
                    raise k_exceptions.NoChildrenForEphemeralsError(
                        "Parent %s is ephemeral" % parent_path)
            path_data = {
                # Kazoo clients expect in milliseconds
                'created_on': utils.millitime(),
                'updated_on': utils.millitime(),
                'version': 0,
                # Not supported for now...
                'aversion': -1,
                'cversion': -1,
                'data': value,
            }
            if ephemeral:
                path_data['ephemeral_owner'] = session_id
                path_data['ephemeral'] = True
            else:
                path_data['ephemeral'] = False
            self._paths[path] = path_data
            return (True, parents, path)

    def pop(self, path):
        if path == ROOT_PATH:
            raise k_exceptions.BadArgumentsError("Can not delete %s"
                                                 % ROOT_PATH)
        with self.lock:
            self._paths.pop(path)

    def get(self, path):
        with self.lock:
            node = self._paths[path]
            children_count = len(self.get_children(path))
            return (node['data'], self._make_znode(node, children_count))

    def __contains__(self, path):
        return path in self._paths

    @contextlib.contextmanager
    def transaction(self):
        with self.lock:
            # Keep the before the transaction information and reset to that
            # data if the context manager fails (this makes it appear that the
            # operations done during the transaction either complete as a
            # group or do not complete).
            paths = self._paths.copy()
            sequences = self._sequences.copy()
            try:
                yield
            except Exception:
                cause = sys.exc_info()
                try:
                    self._paths = paths
                    self._sequences = sequences
                finally:
                    six.reraise(*cause)

    def get_children(self, path, only_direct=True):
        paths = {}
        with self.lock:
            for (other_path, data) in list(six.iteritems(self._paths)):
                if utils.is_child_path(path, other_path,
                                       only_direct=only_direct):
                    paths[other_path] = data
        return paths

    def get_parents(self, path):
        paths = {}
        with self.lock:
            for (other_path, data) in list(six.iteritems(self._paths)):
                if utils.is_child_path(other_path, path, only_direct=False):
                    paths[other_path] = data
        return paths
