# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject

class ClickTrackingTag(AbstractCrudObject):

    class Field(object):
        add_template_param = 'add_template_param'
        ad_id = 'ad_id'
        id = 'id'
        url = 'url'
  
    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'trackingtag'

    def get_node_path(self):
        return (
            self.get_parent_id_assured(),
            self.get_endpoint()
        )

    def remote_delete(self, params=None):
        return self.get_api_assured().call(
            'DELETE',
            self.get_node_path(),
            params=params,
        )
