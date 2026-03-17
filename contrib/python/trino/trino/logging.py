# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
from typing import Optional

LEVEL = logging.INFO


# TODO: provide interface to use ``logging.dictConfig``
def get_logger(name: str, log_level: Optional[int] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    # We must not call setLevel by default except on the root logger otherwise
    # we cannot change log levels for all modules by changing level of the root
    # logger
    if log_level is not None:
        logger.setLevel(log_level)
    return logger


# set default log level to LEVEL
trino_root_logger = get_logger('trino', LEVEL)
