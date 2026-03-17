#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import warnings

from sqlalchemy.sql import functions as sqlfunc

FLATTEN_WARNING = "For backward compatibility params are not rendered."


class flatten(sqlfunc.GenericFunction):
    name = "flatten"

    def __init__(self, *args, **kwargs):
        warnings.warn(FLATTEN_WARNING, DeprecationWarning, stacklevel=2)
        super().__init__(*args, **kwargs)
