#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from .utils import SKIP_IN_PATH, NamespacedClient, _bulk_body, query_params


class TextStructureClient(NamespacedClient):
    @query_params(
        "charset",
        "column_names",
        "delimiter",
        "explain",
        "format",
        "grok_pattern",
        "has_header_row",
        "line_merge_size_limit",
        "lines_to_sample",
        "quote",
        "should_trim_fields",
        "timeout",
        "timestamp_field",
        "timestamp_format",
        request_mimetypes=["application/x-ndjson"],
        response_mimetypes=["application/json"],
    )
    async def find_structure(self, body, params=None, headers=None):
        """
        Finds the structure of a text file. The text file must contain data that is
        suitable to be ingested into Elasticsearch.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/find-structure.html>`_

        :arg body: The contents of the file to be analyzed
        :arg charset: Optional parameter to specify the character set of
            the file
        :arg column_names: Optional parameter containing a comma
            separated list of the column names for a delimited file
        :arg delimiter: Optional parameter to specify the delimiter
            character for a delimited file - must be a single character
        :arg explain: Whether to include a commentary on how the
            structure was derived
        :arg format: Optional parameter to specify the high level file
            format  Valid choices: ndjson, xml, delimited, semi_structured_text
        :arg grok_pattern: Optional parameter to specify the Grok
            pattern that should be used to extract fields from messages in a semi-
            structured text file
        :arg has_header_row: Optional parameter to specify whether a
            delimited file includes the column names in its first row
        :arg line_merge_size_limit: Maximum number of characters
            permitted in a single message when lines are merged to create messages.
            Default: 10000
        :arg lines_to_sample: How many lines of the file should be
            included in the analysis  Default: 1000
        :arg quote: Optional parameter to specify the quote character
            for a delimited file - must be a single character
        :arg should_trim_fields: Optional parameter to specify whether
            the values between delimiters in a delimited file should have whitespace
            trimmed from them
        :arg timeout: Timeout after which the analysis will be aborted
            Default: 25s
        :arg timestamp_field: Optional parameter to specify the
            timestamp field in the file
        :arg timestamp_format: Optional parameter to specify the
            timestamp format in the file - may be either a Joda or Java time format
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return await self.transport.perform_request(
            "POST",
            "/_text_structure/find_structure",
            params=params,
            headers=headers,
            body=body,
        )
