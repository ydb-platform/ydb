# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import wraps

from libcloud.common.types import LibcloudError

__all__ = ["wrap_non_libcloud_exceptions"]


def wrap_non_libcloud_exceptions(func):
    """
    Decorators function which catches non LibcloudError exceptions, wraps them
    in LibcloudError class and re-throws the wrapped exception.

    Note: This function should only be used to wrap methods on the driver
    classes.
    """

    @wraps(func)
    def decorated_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if isinstance(e, LibcloudError):
                raise e

            if len(args) >= 1:
                driver = args[0]
            else:
                driver = None

            fault = getattr(e, "fault", None)

            if fault and getattr(fault, "string", None):
                message = fault.string
            else:
                message = str(e)

            raise LibcloudError(value=message, driver=driver)

    return decorated_function
