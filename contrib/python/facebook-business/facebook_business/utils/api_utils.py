# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

import warnings
from facebook_business import apiconfig
from facebook_business.exceptions import FacebookBadObjectError


def warning(message):
    if apiconfig.ads_api_config['STRICT_MODE']:
        raise FacebookBadObjectError(message)
    else:
        warnings.warn(message)
