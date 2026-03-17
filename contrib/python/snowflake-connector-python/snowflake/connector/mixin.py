#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#


class UnicodeMixin:
    """Mixin class to handle defining the proper __str__/__unicode__ methods in Python 2 or 3."""

    def __str__(self):
        return self.__unicode__()
