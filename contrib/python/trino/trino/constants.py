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
from typing import Any
from typing import Optional

DEFAULT_PORT = 8080
DEFAULT_TLS_PORT = 443
DEFAULT_SOURCE = "trino-python-client"
DEFAULT_CATALOG: Optional[str] = None
DEFAULT_SCHEMA: Optional[str] = None
DEFAULT_AUTH: Optional[Any] = None
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_REQUEST_TIMEOUT: float = 30.0
MAX_NT_PASSWORD_SIZE: int = 1280

HTTP = "http"
HTTPS = "https"

URL_STATEMENT_PATH = "/v1/statement"

CLIENT_NAME = "Trino Python Client"

HEADER_CATALOG = "X-Trino-Catalog"
HEADER_SCHEMA = "X-Trino-Schema"
HEADER_SOURCE = "X-Trino-Source"
HEADER_USER = "X-Trino-User"
HEADER_ORIGINAL_USER = "X-Trino-Original-User"
HEADER_CLIENT_INFO = "X-Trino-Client-Info"
HEADER_CLIENT_TAGS = "X-Trino-Client-Tags"
HEADER_EXTRA_CREDENTIAL = "X-Trino-Extra-Credential"
HEADER_TIMEZONE = "X-Trino-Time-Zone"
HEADER_ENCODING = "X-Trino-Query-Data-Encoding"

HEADER_SESSION = "X-Trino-Session"
HEADER_SET_SESSION = "X-Trino-Set-Session"
HEADER_CLEAR_SESSION = "X-Trino-Clear-Session"

HEADER_ROLE = "X-Trino-Role"
HEADER_SET_ROLE = "X-Trino-Set-Role"

HEADER_STARTED_TRANSACTION = "X-Trino-Started-Transaction-Id"
HEADER_TRANSACTION = "X-Trino-Transaction-Id"

HEADER_PREPARED_STATEMENT = 'X-Trino-Prepared-Statement'
HEADER_ADDED_PREPARE = 'X-Trino-Added-Prepare'
HEADER_DEALLOCATED_PREPARE = 'X-Trino-Deallocated-Prepare'

HEADER_SET_SCHEMA = "X-Trino-Set-Schema"
HEADER_SET_CATALOG = "X-Trino-Set-Catalog"

HEADER_CLIENT_CAPABILITIES = "X-Trino-Client-Capabilities"
CLIENT_CAPABILITY_PARAMETRIC_DATETIME = "PARAMETRIC_DATETIME"
CLIENT_CAPABILITY_SESSION_AUTHORIZATION = "SESSION_AUTHORIZATION"
CLIENT_CAPABILITIES = ','.join([CLIENT_CAPABILITY_PARAMETRIC_DATETIME, CLIENT_CAPABILITY_SESSION_AUTHORIZATION])

HEADER_SET_AUTHORIZATION_USER = "X-Trino-Set-Authorization-User"
HEADER_RESET_AUTHORIZATION_USER = "X-Trino-Reset-Authorization-User"

LENGTH_TYPES = ["char", "varchar"]
PRECISION_TYPES = ["time", "time with time zone", "timestamp", "timestamp with time zone", "decimal"]
SCALE_TYPES = ["decimal"]
