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
        "ConversionOrAdjustmentLagBucketEnum",
    },
)


class ConversionOrAdjustmentLagBucketEnum(proto.Message):
    r"""Container for enum representing the number of days between
    the impression and the conversion or between the impression and
    adjustments to the conversion.

    """

    class ConversionOrAdjustmentLagBucket(proto.Enum):
        r"""Enum representing the number of days between the impression
        and the conversion or between the impression and adjustments to
        the conversion.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CONVERSION_LESS_THAN_ONE_DAY (2):
                Conversion lag bucket from 0 to 1 day. 0 day
                is included, 1 day is not.
            CONVERSION_ONE_TO_TWO_DAYS (3):
                Conversion lag bucket from 1 to 2 days. 1 day
                is included, 2 days is not.
            CONVERSION_TWO_TO_THREE_DAYS (4):
                Conversion lag bucket from 2 to 3 days. 2
                days is included, 3 days is not.
            CONVERSION_THREE_TO_FOUR_DAYS (5):
                Conversion lag bucket from 3 to 4 days. 3
                days is included, 4 days is not.
            CONVERSION_FOUR_TO_FIVE_DAYS (6):
                Conversion lag bucket from 4 to 5 days. 4
                days is included, 5 days is not.
            CONVERSION_FIVE_TO_SIX_DAYS (7):
                Conversion lag bucket from 5 to 6 days. 5
                days is included, 6 days is not.
            CONVERSION_SIX_TO_SEVEN_DAYS (8):
                Conversion lag bucket from 6 to 7 days. 6
                days is included, 7 days is not.
            CONVERSION_SEVEN_TO_EIGHT_DAYS (9):
                Conversion lag bucket from 7 to 8 days. 7
                days is included, 8 days is not.
            CONVERSION_EIGHT_TO_NINE_DAYS (10):
                Conversion lag bucket from 8 to 9 days. 8
                days is included, 9 days is not.
            CONVERSION_NINE_TO_TEN_DAYS (11):
                Conversion lag bucket from 9 to 10 days. 9
                days is included, 10 days is not.
            CONVERSION_TEN_TO_ELEVEN_DAYS (12):
                Conversion lag bucket from 10 to 11 days. 10
                days is included, 11 days is not.
            CONVERSION_ELEVEN_TO_TWELVE_DAYS (13):
                Conversion lag bucket from 11 to 12 days. 11
                days is included, 12 days is not.
            CONVERSION_TWELVE_TO_THIRTEEN_DAYS (14):
                Conversion lag bucket from 12 to 13 days. 12
                days is included, 13 days is not.
            CONVERSION_THIRTEEN_TO_FOURTEEN_DAYS (15):
                Conversion lag bucket from 13 to 14 days. 13
                days is included, 14 days is not.
            CONVERSION_FOURTEEN_TO_TWENTY_ONE_DAYS (16):
                Conversion lag bucket from 14 to 21 days. 14
                days is included, 21 days is not.
            CONVERSION_TWENTY_ONE_TO_THIRTY_DAYS (17):
                Conversion lag bucket from 21 to 30 days. 21
                days is included, 30 days is not.
            CONVERSION_THIRTY_TO_FORTY_FIVE_DAYS (18):
                Conversion lag bucket from 30 to 45 days. 30
                days is included, 45 days is not.
            CONVERSION_FORTY_FIVE_TO_SIXTY_DAYS (19):
                Conversion lag bucket from 45 to 60 days. 45
                days is included, 60 days is not.
            CONVERSION_SIXTY_TO_NINETY_DAYS (20):
                Conversion lag bucket from 60 to 90 days. 60
                days is included, 90 days is not.
            ADJUSTMENT_LESS_THAN_ONE_DAY (21):
                Conversion adjustment lag bucket from 0 to 1
                day. 0 day is included, 1 day is not.
            ADJUSTMENT_ONE_TO_TWO_DAYS (22):
                Conversion adjustment lag bucket from 1 to 2
                days. 1 day is included, 2 days is not.
            ADJUSTMENT_TWO_TO_THREE_DAYS (23):
                Conversion adjustment lag bucket from 2 to 3
                days. 2 days is included, 3 days is not.
            ADJUSTMENT_THREE_TO_FOUR_DAYS (24):
                Conversion adjustment lag bucket from 3 to 4
                days. 3 days is included, 4 days is not.
            ADJUSTMENT_FOUR_TO_FIVE_DAYS (25):
                Conversion adjustment lag bucket from 4 to 5
                days. 4 days is included, 5 days is not.
            ADJUSTMENT_FIVE_TO_SIX_DAYS (26):
                Conversion adjustment lag bucket from 5 to 6
                days. 5 days is included, 6 days is not.
            ADJUSTMENT_SIX_TO_SEVEN_DAYS (27):
                Conversion adjustment lag bucket from 6 to 7
                days. 6 days is included, 7 days is not.
            ADJUSTMENT_SEVEN_TO_EIGHT_DAYS (28):
                Conversion adjustment lag bucket from 7 to 8
                days. 7 days is included, 8 days is not.
            ADJUSTMENT_EIGHT_TO_NINE_DAYS (29):
                Conversion adjustment lag bucket from 8 to 9
                days. 8 days is included, 9 days is not.
            ADJUSTMENT_NINE_TO_TEN_DAYS (30):
                Conversion adjustment lag bucket from 9 to 10
                days. 9 days is included, 10 days is not.
            ADJUSTMENT_TEN_TO_ELEVEN_DAYS (31):
                Conversion adjustment lag bucket from 10 to
                11 days. 10 days is included, 11 days is not.
            ADJUSTMENT_ELEVEN_TO_TWELVE_DAYS (32):
                Conversion adjustment lag bucket from 11 to
                12 days. 11 days is included, 12 days is not.
            ADJUSTMENT_TWELVE_TO_THIRTEEN_DAYS (33):
                Conversion adjustment lag bucket from 12 to
                13 days. 12 days is included, 13 days is not.
            ADJUSTMENT_THIRTEEN_TO_FOURTEEN_DAYS (34):
                Conversion adjustment lag bucket from 13 to
                14 days. 13 days is included, 14 days is not.
            ADJUSTMENT_FOURTEEN_TO_TWENTY_ONE_DAYS (35):
                Conversion adjustment lag bucket from 14 to
                21 days. 14 days is included, 21 days is not.
            ADJUSTMENT_TWENTY_ONE_TO_THIRTY_DAYS (36):
                Conversion adjustment lag bucket from 21 to
                30 days. 21 days is included, 30 days is not.
            ADJUSTMENT_THIRTY_TO_FORTY_FIVE_DAYS (37):
                Conversion adjustment lag bucket from 30 to
                45 days. 30 days is included, 45 days is not.
            ADJUSTMENT_FORTY_FIVE_TO_SIXTY_DAYS (38):
                Conversion adjustment lag bucket from 45 to
                60 days. 45 days is included, 60 days is not.
            ADJUSTMENT_SIXTY_TO_NINETY_DAYS (39):
                Conversion adjustment lag bucket from 60 to
                90 days. 60 days is included, 90 days is not.
            ADJUSTMENT_NINETY_TO_ONE_HUNDRED_AND_FORTY_FIVE_DAYS (40):
                Conversion adjustment lag bucket from 90 to
                145 days. 90 days is included, 145 days is not.
            CONVERSION_UNKNOWN (41):
                Conversion lag bucket UNKNOWN. This is for
                dates before conversion lag bucket was available
                in Google Ads.
            ADJUSTMENT_UNKNOWN (42):
                Conversion adjustment lag bucket UNKNOWN.
                This is for dates before conversion adjustment
                lag bucket was available in Google Ads.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONVERSION_LESS_THAN_ONE_DAY = 2
        CONVERSION_ONE_TO_TWO_DAYS = 3
        CONVERSION_TWO_TO_THREE_DAYS = 4
        CONVERSION_THREE_TO_FOUR_DAYS = 5
        CONVERSION_FOUR_TO_FIVE_DAYS = 6
        CONVERSION_FIVE_TO_SIX_DAYS = 7
        CONVERSION_SIX_TO_SEVEN_DAYS = 8
        CONVERSION_SEVEN_TO_EIGHT_DAYS = 9
        CONVERSION_EIGHT_TO_NINE_DAYS = 10
        CONVERSION_NINE_TO_TEN_DAYS = 11
        CONVERSION_TEN_TO_ELEVEN_DAYS = 12
        CONVERSION_ELEVEN_TO_TWELVE_DAYS = 13
        CONVERSION_TWELVE_TO_THIRTEEN_DAYS = 14
        CONVERSION_THIRTEEN_TO_FOURTEEN_DAYS = 15
        CONVERSION_FOURTEEN_TO_TWENTY_ONE_DAYS = 16
        CONVERSION_TWENTY_ONE_TO_THIRTY_DAYS = 17
        CONVERSION_THIRTY_TO_FORTY_FIVE_DAYS = 18
        CONVERSION_FORTY_FIVE_TO_SIXTY_DAYS = 19
        CONVERSION_SIXTY_TO_NINETY_DAYS = 20
        ADJUSTMENT_LESS_THAN_ONE_DAY = 21
        ADJUSTMENT_ONE_TO_TWO_DAYS = 22
        ADJUSTMENT_TWO_TO_THREE_DAYS = 23
        ADJUSTMENT_THREE_TO_FOUR_DAYS = 24
        ADJUSTMENT_FOUR_TO_FIVE_DAYS = 25
        ADJUSTMENT_FIVE_TO_SIX_DAYS = 26
        ADJUSTMENT_SIX_TO_SEVEN_DAYS = 27
        ADJUSTMENT_SEVEN_TO_EIGHT_DAYS = 28
        ADJUSTMENT_EIGHT_TO_NINE_DAYS = 29
        ADJUSTMENT_NINE_TO_TEN_DAYS = 30
        ADJUSTMENT_TEN_TO_ELEVEN_DAYS = 31
        ADJUSTMENT_ELEVEN_TO_TWELVE_DAYS = 32
        ADJUSTMENT_TWELVE_TO_THIRTEEN_DAYS = 33
        ADJUSTMENT_THIRTEEN_TO_FOURTEEN_DAYS = 34
        ADJUSTMENT_FOURTEEN_TO_TWENTY_ONE_DAYS = 35
        ADJUSTMENT_TWENTY_ONE_TO_THIRTY_DAYS = 36
        ADJUSTMENT_THIRTY_TO_FORTY_FIVE_DAYS = 37
        ADJUSTMENT_FORTY_FIVE_TO_SIXTY_DAYS = 38
        ADJUSTMENT_SIXTY_TO_NINETY_DAYS = 39
        ADJUSTMENT_NINETY_TO_ONE_HUNDRED_AND_FORTY_FIVE_DAYS = 40
        CONVERSION_UNKNOWN = 41
        ADJUSTMENT_UNKNOWN = 42


__all__ = tuple(sorted(__protobuf__.manifest))
