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

import json

from pylxd.deprecated import base


class LXDCertificate(base.LXDBase):

    def certificate_list(self):
        (state, data) = self.connection.get_object('GET', '/1.0/certificates')
        return [certificate.split('/1.0/certificates/')[-1]
                for certificate in data['metadata']]

    def certificate_show(self, fingerprint):
        return self.connection.get_object('GET', '/1.0/certificates/%s'
                                          % fingerprint)

    def certificate_create(self, certificate):
        return self.connection.get_status('POST', '/1.0/certificates',
                                          json.dumps(certificate))

    def certificate_delete(self, fingerprint):
        return self.connection.get_status('DELETE', '/1.0/certificates/%s'
                                          % fingerprint)
