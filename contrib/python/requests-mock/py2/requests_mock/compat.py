# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


class _FakeHTTPMessage(object):

    def __init__(self, headers):
        self.headers = headers

    def getheaders(self, name):
        try:
            return [self.headers[name]]
        except KeyError:
            return []

    def get_all(self, name, failobj=None):
        # python 3 only, overrides email.message.Message.get_all
        try:
            return [self.headers[name]]
        except KeyError:
            return failobj
