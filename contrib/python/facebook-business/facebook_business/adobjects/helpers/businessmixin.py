# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.adsinsights import AdsInsights

class BusinessMixin:

    def get_insights(self, fields=None, params=None, is_async=False):
        return self.iterate_edge_async(
            AdsInsights,
            fields,
            params,
            is_async,
            include_summary=False,
        )
