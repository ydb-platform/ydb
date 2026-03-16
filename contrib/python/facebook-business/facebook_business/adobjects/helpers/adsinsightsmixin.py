# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

class AdsInsightsMixin:

    class Increment(object):
        monthly = 'monthly'
        all_days = 'all_days'

    class Operator(object):
        all = 'ALL'
        any = 'ANY'
        contain = 'CONTAIN'
        equal = 'EQUAL'
        greater_than = 'GREATER_THAN'
        greater_than_or_equal = 'GREATER_THAN_OR_EQUAL'
        in_ = 'IN'
        in_range = 'IN_RANGE'
        less_than = 'LESS_THAN'
        less_than_or_equal = 'LESS_THAN_OR_EQUAL'
        none = 'NONE'
        not_contain = 'NOT_CONTAIN'
        not_equal = 'NOT_EQUAL'
        not_in = 'NOT_IN'
        not_in_range = 'NOT_IN_RANGE'
