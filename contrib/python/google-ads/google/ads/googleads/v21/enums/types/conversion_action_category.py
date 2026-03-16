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
        "ConversionActionCategoryEnum",
    },
)


class ConversionActionCategoryEnum(proto.Message):
    r"""Container for enum describing the category of conversions
    that are associated with a ConversionAction.

    """

    class ConversionActionCategory(proto.Enum):
        r"""The category of conversions that are associated with a
        ConversionAction.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            DEFAULT (2):
                Default category.
            PAGE_VIEW (3):
                User visiting a page.
            PURCHASE (4):
                Purchase, sales, or "order placed" event.
            SIGNUP (5):
                Signup user action.
            DOWNLOAD (7):
                Software download action (as for an app).
            ADD_TO_CART (8):
                The addition of items to a shopping cart or
                bag on an advertiser site.
            BEGIN_CHECKOUT (9):
                When someone enters the checkout flow on an
                advertiser site.
            SUBSCRIBE_PAID (10):
                The start of a paid subscription for a
                product or service.
            PHONE_CALL_LEAD (11):
                A call to indicate interest in an
                advertiser's offering.
            IMPORTED_LEAD (12):
                A lead conversion imported from an external
                source into Google Ads.
            SUBMIT_LEAD_FORM (13):
                A submission of a form on an advertiser site
                indicating business interest.
            BOOK_APPOINTMENT (14):
                A booking of an appointment with an
                advertiser's business.
            REQUEST_QUOTE (15):
                A quote or price estimate request.
            GET_DIRECTIONS (16):
                A search for an advertiser's business
                location with intention to visit.
            OUTBOUND_CLICK (17):
                A click to an advertiser's partner's site.
            CONTACT (18):
                A call, SMS, email, chat or other type of
                contact to an advertiser.
            ENGAGEMENT (19):
                A website engagement event such as long site
                time or a Google Analytics (GA) Smart Goal.
                Intended to be used for GA, Firebase, GA Gold
                goal imports.
            STORE_VISIT (20):
                A visit to a physical store location.
            STORE_SALE (21):
                A sale occurring in a physical store.
            QUALIFIED_LEAD (22):
                A lead conversion imported from an external
                source into Google Ads, that has been further
                qualified by the advertiser (marketing/sales
                team). In the lead-to-sale journey, advertisers
                get leads, then act on them by reaching out to
                the consumer. If the consumer is interested and
                may end up buying their product, the advertiser
                marks such leads as "qualified leads".
            CONVERTED_LEAD (23):
                A lead conversion imported from an external
                source into Google Ads, that has further
                completed a chosen stage as defined by the lead
                gen advertiser.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DEFAULT = 2
        PAGE_VIEW = 3
        PURCHASE = 4
        SIGNUP = 5
        DOWNLOAD = 7
        ADD_TO_CART = 8
        BEGIN_CHECKOUT = 9
        SUBSCRIBE_PAID = 10
        PHONE_CALL_LEAD = 11
        IMPORTED_LEAD = 12
        SUBMIT_LEAD_FORM = 13
        BOOK_APPOINTMENT = 14
        REQUEST_QUOTE = 15
        GET_DIRECTIONS = 16
        OUTBOUND_CLICK = 17
        CONTACT = 18
        ENGAGEMENT = 19
        STORE_VISIT = 20
        STORE_SALE = 21
        QUALIFIED_LEAD = 22
        CONVERTED_LEAD = 23


__all__ = tuple(sorted(__protobuf__.manifest))
