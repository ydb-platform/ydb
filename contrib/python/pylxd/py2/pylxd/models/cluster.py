# Copyright (c) 2016 Canonical Ltd
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
from pylxd.models import _model as model
from pylxd import managers


class Cluster(model.Model):
    """An LXD Cluster.
    """

    server_name = model.Attribute()
    enabled = model.Attribute()
    member_config = model.Attribute()

    members = model.Manager()

    def __init__(self, *args, **kwargs):
        super(Cluster, self).__init__(*args, **kwargs)
        self.members = managers.ClusterMemberManager(self.client, self)

    @property
    def api(self):
        return self.client.api.cluster

    @classmethod
    def get(cls, client, *args):
        """Get cluster details"""
        print(args)
        response = client.api.cluster.get()
        print(response.json())
        container = cls(client, **response.json()['metadata'])
        return container


class ClusterMember(model.Model):
    """A LXD cluster member."""

    url = model.Attribute(readonly=True)
    database = model.Attribute(readonly=True)
    server_name = model.Attribute(readonly=True)
    status = model.Attribute(readonly=True)
    message = model.Attribute(readonly=True)

    cluster = model.Parent()

    @classmethod
    def get(cls, client, server_name):
        """Get a cluster member by name."""
        response = client.api.cluster.members[server_name].get()

        return cls(client, **response.json()['metadata'])

    @classmethod
    def all(cls, client, *args):
        """Get all cluster members."""
        response = client.api.cluster.members.get()

        nodes = []
        for node in response.json()['metadata']:
            server_name = node.split('/')[-1]
            nodes.append(cls(client, server_name=server_name))
        return nodes

    @property
    def api(self):
        return self.client.api.cluster.members[self.server_name]
