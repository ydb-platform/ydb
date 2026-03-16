# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

class ReachFrequencyPredictionMixin:
    def reserve(
        self,
        prediction_to_release=None,
        reach=None,
        budget=None,
        impression=None,
    ):
        params = {
            self.Field.prediction_id: self.get_id_assured(),
            self.Field.prediction_id_to_release: prediction_to_release,
            self.Field.budget: budget,
            self.Field.reach: reach,
            self.Field.impression: impression,
            self.Field.action: self.Action.reserve,
        }
        # Filter out None values.
        params = {k: v for k, v in params.items() if v is not None}

        response = self.get_api_assured().call(
            'POST',
            (self.get_parent_id_assured(), self.get_endpoint()),
            params=params,
        )

        return self.__class__(response.body(), self.get_parent_id_assured())

    def cancel(self):
        params = {
            self.Field.prediction_id: self.get_id_assured(),
            self.Field.action: self.Action.cancel,
        }
        self.get_api_assured().call(
            'POST',
            (self.get_parent_id_assured(), self.get_endpoint()),
            params=params,
        )
        return self
