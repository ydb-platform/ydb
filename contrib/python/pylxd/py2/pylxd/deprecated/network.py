# Copyright (c) 2015 Canonical Ltd
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

from pylxd.deprecated import base


class LXDNetwork(base.LXDBase):

    def network_list(self):
        (state, data) = self.connection.get_object('GET', '/1.0/networks')
        return [network.split('/1.0/networks/')[-1]
                for network in data['metadata']]

    def network_show(self, network):
        '''Show details of the LXD network'''
        (state, data) = self.connection.get_object('GET', '/1.0/networks/%s'
                                                   % network)
        return {
            'network_name':
                self.show_network_name(network, data.get('metadata')),
            'network_type':
                self.show_network_type(network, data.get('metadata')),
            'network_members':
                self.show_network_members(network, data.get('metadata'))
        }

    def show_network_name(self, network, data):
        '''Show the LXD network name'''
        if data is None:
            (state, data) = self.connection.get_object(
                'GET', '/1.0/networks/%s' % network)
            data = data.get('metadata')
        return data['name']

    def show_network_type(self, network, data):
        if data is None:
            (state, data) = self.connection.get_object(
                'GET', '/1.0/networks/%s' % network)
            data = data.get('metadata')
        return data['type']

    def show_network_members(self, network, data):
        if data is None:
            (state, data) = self.connection.get_object(
                'GET', '/1.0/networks/%s' % network)
            data = data.get('metadata')
        return [network_members.split('/1.0/networks/')[-1]
                for network_members in data['members']]
