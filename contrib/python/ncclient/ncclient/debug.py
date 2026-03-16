# Copyright 2009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ncclient.transport import SessionListener

class PrintListener(SessionListener):

    def callback(self, root, raw):
        print('\n# RECEIVED MESSAGE with root=[tag=%r, attrs=%r] #\n%r\n' %
              (root[0], root[1], raw))

    def errback(self, err):
        print('\n# RECEIVED ERROR #\n%r\n' % err)
