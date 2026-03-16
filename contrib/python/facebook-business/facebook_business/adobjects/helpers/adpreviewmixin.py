# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

class AdPreviewMixin:
    def get_html(self):
        """Returns the preview html."""
        return self[self.Field.body]
