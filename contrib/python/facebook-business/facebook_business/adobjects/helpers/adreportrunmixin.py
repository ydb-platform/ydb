# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

class AdReportRunMixin:

    def get_result(self, fields=None, params=None):
        """
        Gets the final result from an async job
        Accepts params such as limit
        """
        return self.get_insights(fields=fields, params=params)

    def __nonzero__(self):
        return self[self.Field.async_percent_completion] == 100

    def _setitem_trigger(self, key, value):
        if key == 'report_run_id':
            self._data['id'] = self['report_run_id']
