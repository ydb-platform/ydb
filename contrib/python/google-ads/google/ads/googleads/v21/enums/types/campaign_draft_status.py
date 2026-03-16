# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations


import proto  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "CampaignDraftStatusEnum",
    },
)


class CampaignDraftStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of a campaign
    draft.

    """

    class CampaignDraftStatus(proto.Enum):
        r"""Possible statuses of a campaign draft.

        Values:
            UNSPECIFIED (0):
                The status has not been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PROPOSED (2):
                Initial state of the draft, the advertiser
                can start adding changes with no effect on
                serving.
            REMOVED (3):
                The campaign draft is removed.
            PROMOTING (5):
                Advertiser requested to promote draft's
                changes back into the original campaign.
                Advertiser can poll the long running operation
                returned by the promote action to see the status
                of the promotion.
            PROMOTED (4):
                The process to merge changes in the draft
                back to the original campaign has completed
                successfully.
            PROMOTE_FAILED (6):
                The promotion failed after it was partially
                applied. Promote cannot be attempted again
                safely, so the issue must be corrected in the
                original campaign.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PROPOSED = 2
        REMOVED = 3
        PROMOTING = 5
        PROMOTED = 4
        PROMOTE_FAILED = 6


__all__ = tuple(sorted(__protobuf__.manifest))
