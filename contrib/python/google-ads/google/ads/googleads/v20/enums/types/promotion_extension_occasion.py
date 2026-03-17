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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "PromotionExtensionOccasionEnum",
    },
)


class PromotionExtensionOccasionEnum(proto.Message):
    r"""Container for enum describing a promotion extension occasion.
    For more information about the occasions  check:

    https://support.google.com/google-ads/answer/7367521

    """

    class PromotionExtensionOccasion(proto.Enum):
        r"""A promotion extension occasion.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            NEW_YEARS (2):
                New Year's.
            CHINESE_NEW_YEAR (3):
                Chinese New Year.
            VALENTINES_DAY (4):
                Valentine's Day.
            EASTER (5):
                Easter.
            MOTHERS_DAY (6):
                Mother's Day.
            FATHERS_DAY (7):
                Father's Day.
            LABOR_DAY (8):
                Labor Day.
            BACK_TO_SCHOOL (9):
                Back To School.
            HALLOWEEN (10):
                Halloween.
            BLACK_FRIDAY (11):
                Black Friday.
            CYBER_MONDAY (12):
                Cyber Monday.
            CHRISTMAS (13):
                Christmas.
            BOXING_DAY (14):
                Boxing Day.
            INDEPENDENCE_DAY (15):
                Independence Day in any country.
            NATIONAL_DAY (16):
                National Day in any country.
            END_OF_SEASON (17):
                End of any season.
            WINTER_SALE (18):
                Winter Sale.
            SUMMER_SALE (19):
                Summer sale.
            FALL_SALE (20):
                Fall Sale.
            SPRING_SALE (21):
                Spring Sale.
            RAMADAN (22):
                Ramadan.
            EID_AL_FITR (23):
                Eid al-Fitr.
            EID_AL_ADHA (24):
                Eid al-Adha.
            SINGLES_DAY (25):
                Singles Day.
            WOMENS_DAY (26):
                Women's Day.
            HOLI (27):
                Holi.
            PARENTS_DAY (28):
                Parent's Day.
            ST_NICHOLAS_DAY (29):
                St. Nicholas Day.
            CARNIVAL (30):
                Carnival.
            EPIPHANY (31):
                Epiphany, also known as Three Kings' Day.
            ROSH_HASHANAH (32):
                Rosh Hashanah.
            PASSOVER (33):
                Passover.
            HANUKKAH (34):
                Hanukkah.
            DIWALI (35):
                Diwali.
            NAVRATRI (36):
                Navratri.
            SONGKRAN (37):
                Available in Thai: Songkran.
            YEAR_END_GIFT (38):
                Available in Japanese: Year-end Gift.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NEW_YEARS = 2
        CHINESE_NEW_YEAR = 3
        VALENTINES_DAY = 4
        EASTER = 5
        MOTHERS_DAY = 6
        FATHERS_DAY = 7
        LABOR_DAY = 8
        BACK_TO_SCHOOL = 9
        HALLOWEEN = 10
        BLACK_FRIDAY = 11
        CYBER_MONDAY = 12
        CHRISTMAS = 13
        BOXING_DAY = 14
        INDEPENDENCE_DAY = 15
        NATIONAL_DAY = 16
        END_OF_SEASON = 17
        WINTER_SALE = 18
        SUMMER_SALE = 19
        FALL_SALE = 20
        SPRING_SALE = 21
        RAMADAN = 22
        EID_AL_FITR = 23
        EID_AL_ADHA = 24
        SINGLES_DAY = 25
        WOMENS_DAY = 26
        HOLI = 27
        PARENTS_DAY = 28
        ST_NICHOLAS_DAY = 29
        CARNIVAL = 30
        EPIPHANY = 31
        ROSH_HASHANAH = 32
        PASSOVER = 33
        HANUKKAH = 34
        DIWALI = 35
        NAVRATRI = 36
        SONGKRAN = 37
        YEAR_END_GIFT = 38


__all__ = tuple(sorted(__protobuf__.manifest))
