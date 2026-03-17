
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

from spdx.utils import datetime_iso_format


@total_ordering
class Review(object):

    """
    Document review information.
    Fields:
    - reviewer: Person, Organization or tool that reviewed the SPDX file.
      Mandatory one.
    - review_date: Review date, mandatory one. Type: datetime.
    - comment: Review comment. Optional one. Type: str.
    """

    def __init__(self, reviewer=None, review_date=None, comment=None):
        self.reviewer = reviewer
        self.review_date = review_date
        self.comment = comment

    def __eq__(self, other):
        return (
            isinstance(other, Review) and self.reviewer == other.reviewer
            and self.review_date == other.review_date
            and self.comment == other.comment
        )

    def __lt__(self, other):
        return (
            (self.reviewer, self.review_date, self.comment) <
            (other.reviewer, other.review_date, other.comment,)
        )

    def set_review_date_now(self):
        self.review_date = datetime.utcnow()

    @property
    def review_date_iso_format(self):
        return datetime_iso_format(self.review_date)

    @property
    def has_comment(self):
        return self.comment is not None

    def validate(self, messages):
        """Returns True if all the fields are valid.
        Appends any error messages to messages parameter.
        """
        messages = self.validate_reviewer(messages)
        messages = self.validate_review_date(messages)

        return messages

    def validate_reviewer(self, messages):
        if self.reviewer is None:
            messages = messages + ['Review missing reviewer.']

        return messages

    def validate_review_date(self, messages):
        if self.review_date is None:
            messages = messages + ['Review missing review date.']

        return messages
