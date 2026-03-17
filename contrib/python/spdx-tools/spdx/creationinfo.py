
# Copyright (c) 2014 Ahmed H. Ismail
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
from functools import total_ordering

from spdx import config
from spdx import utils


@total_ordering
class Creator(object):
    """
    Creator enity.
    Fields:
    - name: creator's name/identifier
    """
    def __init__(self, name):
        self.name = name

    # FIXME: do not overrride eq and not hash
    def __eq__(self, other):
        return isinstance(other, Creator) and self.name == other.name

    def __lt__(self, other):
        return isinstance(other, Creator) and self.name < other.name


@total_ordering
class Organization(Creator):
    """
    Organization entity.
    Fields:
    - name: Org's name/identifier. Mandatory. Type: str.
    - email: Org's email address. Optional. Type: str.
    """

    def __init__(self, name, email):
        super(Organization, self).__init__(name)
        self.email = email

    # FIXME: do not overrride eq and not hash
    def __eq__(self, other):
        return isinstance(other, Organization) and (self.name, self.email) == (other.name, other.email)

    def __lt__(self, other):
        return isinstance(other, Organization) and (self.name, self.email) < (other.name, other.email)

    def to_value(self):
        if self.email:
            return 'Organization: {0} ({1})'.format(self.name, self.email)
        else:
            return 'Organization: {0}'.format(self.name)

    def __str__(self):
        return self.to_value()


@total_ordering
class Person(Creator):
    """
    Person entity.
    Fields:
    - name: person's name/identifier. Mandatory. Type: str.
    - email: person's email address. Optional. Type: str.
    """

    def __init__(self, name, email):
        super(Person, self).__init__(name)
        self.email = email

    # FIXME: do not overrride eq and not hash
    def __eq__(self, other):
        return isinstance(other, Person) and (self.name, self.email) == (other.name, other.email)

    def __lt__(self, other):
        return isinstance(other, Person) and (self.name, self.email) < (other.name, other.email)

    def to_value(self):
        if self.email is not None:
            return 'Person: {0} ({1})'.format(self.name, self.email)
        else:
            return 'Person: {0}'.format(self.name)

    def __str__(self):
        return self.to_value()


class Tool(Creator):
    """
    Tool entity.
    Fields:
    - name: tool identifier, with version. Type: str.
    """

    def __init__(self, name):
        super(Tool, self).__init__(name)

    def to_value(self):
        return 'Tool: {0}'.format(self.name)

    def __str__(self):
        return self.to_value()


class CreationInfo(object):
    """
    Represent a document creation info.
    Fields:
    - creators: List of creators. At least one required.
        Type: Creator.
    - comment: Creation comment, optional. Type: str.
    - license_list_version: version of SPDX license used in creation of SPDX
        document. One, optional. Type: spdx.version.Version
    - created: Creation date. Mandatory one. Type: datetime.
    """

    def __init__(self, created=None, comment=None,
                 license_list_version=config.LICENSE_LIST_VERSION):
        self.creators = []
        self.created = created
        self.comment = comment
        self.license_list_version = license_list_version

    def add_creator(self, creator):
        self.creators.append(creator)

    def remove_creator(self, creator):
        self.creators.remove(creator)

    def set_created_now(self):
        self.created = datetime.utcnow()

    @property
    def created_iso_format(self):
        return utils.datetime_iso_format(self.created)

    @property
    def has_comment(self):
        return self.comment is not None

    def validate(self, messages):
        """Returns True if the fields are valid according to the SPDX standard.
        Appends user friendly messages to the messages parameter.
        """
        messages = self.validate_creators(messages)
        messages = self.validate_created(messages)

        return messages

    def validate_creators(self, messages):
        if len(self.creators) == 0:
            messages = messages + [
                'No creators defined, must have at least one.']

        return messages

    def validate_created(self, messages):
        if self.created is None:
            messages = messages + ['Creation info missing created date.']

        return messages
