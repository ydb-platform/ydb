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
from trino.exceptions import TrinoQueryError  # noqa

# ref: https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/StandardErrorCode.java
NOT_FOUND = "NOT_FOUND"
COLUMN_NOT_FOUND = "COLUMN_NOT_FOUND"
TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
SCHEMA_NOT_FOUND = "SCHEMA_NOT_FOUND"
CATALOG_NOT_FOUND = "CATALOG_NOT_FOUND"

MISSING_TABLE = "MISSING_TABLE"
MISSING_COLUMN_NAME = "MISSING_COLUMN_NAME"
MISSING_SCHEMA_NAME = "MISSING_SCHEMA_NAME"
MISSING_CATALOG_NAME = "MISSING_CATALOG_NAME"

PERMISSION_DENIED = "PERMISSION_DENIED"
