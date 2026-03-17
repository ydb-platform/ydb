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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "AssetGroupListingGroupFilterErrorEnum",
    },
)


class AssetGroupListingGroupFilterErrorEnum(proto.Message):
    r"""Container for enum describing possible asset group listing
    group filter errors.

    """

    class AssetGroupListingGroupFilterError(proto.Enum):
        r"""Enum describing possible asset group listing group filter
        errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            TREE_TOO_DEEP (2):
                Listing group tree is too deep.
            UNIT_CANNOT_HAVE_CHILDREN (3):
                Listing Group UNIT node cannot have children.
            SUBDIVISION_MUST_HAVE_EVERYTHING_ELSE_CHILD (4):
                Listing Group SUBDIVISION node must have
                everything else child.
            DIFFERENT_DIMENSION_TYPE_BETWEEN_SIBLINGS (5):
                Dimension type of Listing Group must be the
                same as that of its siblings.
            SAME_DIMENSION_VALUE_BETWEEN_SIBLINGS (6):
                The sibling Listing Groups target exactly the
                same dimension value.
            SAME_DIMENSION_TYPE_BETWEEN_ANCESTORS (7):
                The dimension type is the same as one of the
                ancestor Listing Groups.
            MULTIPLE_ROOTS (8):
                Each Listing Group tree must have a single
                root.
            INVALID_DIMENSION_VALUE (9):
                Invalid Listing Group dimension value.
            MUST_REFINE_HIERARCHICAL_PARENT_TYPE (10):
                Hierarchical dimension must refine a
                dimension of the same type.
            INVALID_PRODUCT_BIDDING_CATEGORY (11):
                Invalid Product Bidding Category.
            CHANGING_CASE_VALUE_WITH_CHILDREN (12):
                Modifying case value is allowed only while
                updating the entire subtree at the same time.
            SUBDIVISION_HAS_CHILDREN (13):
                Subdivision node has children which must be
                removed first.
            CANNOT_REFINE_HIERARCHICAL_EVERYTHING_ELSE (14):
                Dimension can't subdivide everything-else
                node in its own hierarchy.
            DIMENSION_TYPE_NOT_ALLOWED (15):
                This dimension type is not allowed in this
                context.
            DUPLICATE_WEBPAGE_FILTER_UNDER_ASSET_GROUP (16):
                All the webpage filters under an AssetGroup
                should be distinct.
            LISTING_SOURCE_NOT_ALLOWED (17):
                Filter of the listing source type is not
                allowed in the context.
            FILTER_EXCLUSION_NOT_ALLOWED (18):
                Exclusion filters are not allowed in the
                context.
            MULTIPLE_LISTING_SOURCES (19):
                All the filters under an AssetGroup should
                have the same listing source.
            MULTIPLE_WEBPAGE_CONDITION_TYPES_NOT_ALLOWED (20):
                All the conditions in a webpage needs to be
                of same type.
            MULTIPLE_WEBPAGE_TYPES_PER_ASSET_GROUP (21):
                All the webpage types of the filters under an AssetGroup
                should be of same type. Example: All the webpage types can
                be of type custom_label or url_contains but not both.
            PAGE_FEED_FILTER_HAS_PARENT (22):
                All page feed filter nodes are root nodes and
                they can't have a parent.
            MULTIPLE_OPERATIONS_ON_ONE_NODE (23):
                There cannot be more than one mutate
                operation per request that targets a single
                asset group listing group filter.
            TREE_WAS_INVALID_BEFORE_MUTATION (24):
                The tree is in an invalid state in the
                database. Any changes that don't fix its issues
                will fail validation.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TREE_TOO_DEEP = 2
        UNIT_CANNOT_HAVE_CHILDREN = 3
        SUBDIVISION_MUST_HAVE_EVERYTHING_ELSE_CHILD = 4
        DIFFERENT_DIMENSION_TYPE_BETWEEN_SIBLINGS = 5
        SAME_DIMENSION_VALUE_BETWEEN_SIBLINGS = 6
        SAME_DIMENSION_TYPE_BETWEEN_ANCESTORS = 7
        MULTIPLE_ROOTS = 8
        INVALID_DIMENSION_VALUE = 9
        MUST_REFINE_HIERARCHICAL_PARENT_TYPE = 10
        INVALID_PRODUCT_BIDDING_CATEGORY = 11
        CHANGING_CASE_VALUE_WITH_CHILDREN = 12
        SUBDIVISION_HAS_CHILDREN = 13
        CANNOT_REFINE_HIERARCHICAL_EVERYTHING_ELSE = 14
        DIMENSION_TYPE_NOT_ALLOWED = 15
        DUPLICATE_WEBPAGE_FILTER_UNDER_ASSET_GROUP = 16
        LISTING_SOURCE_NOT_ALLOWED = 17
        FILTER_EXCLUSION_NOT_ALLOWED = 18
        MULTIPLE_LISTING_SOURCES = 19
        MULTIPLE_WEBPAGE_CONDITION_TYPES_NOT_ALLOWED = 20
        MULTIPLE_WEBPAGE_TYPES_PER_ASSET_GROUP = 21
        PAGE_FEED_FILTER_HAS_PARENT = 22
        MULTIPLE_OPERATIONS_ON_ONE_NODE = 23
        TREE_WAS_INVALID_BEFORE_MUTATION = 24


__all__ = tuple(sorted(__protobuf__.manifest))
