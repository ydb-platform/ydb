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

import typing as t

from elastic_transport import ObjectApiResponse

from ._base import NamespacedClient
from .utils import _rewrite_parameters


class TextStructureClient(NamespacedClient):

    @_rewrite_parameters()
    async def find_field_structure(
        self,
        *,
        field: str,
        index: str,
        column_names: t.Optional[str] = None,
        delimiter: t.Optional[str] = None,
        documents_to_sample: t.Optional[int] = None,
        ecs_compatibility: t.Optional[t.Union[str, t.Literal["disabled", "v1"]]] = None,
        error_trace: t.Optional[bool] = None,
        explain: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[
            t.Union[
                str, t.Literal["delimited", "ndjson", "semi_structured_text", "xml"]
            ]
        ] = None,
        grok_pattern: t.Optional[str] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        quote: t.Optional[str] = None,
        should_trim_fields: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        timestamp_field: t.Optional[str] = None,
        timestamp_format: t.Optional[str] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Find the structure of a text field.
          Find the structure of a text field in an Elasticsearch index.</p>
          <p>This API provides a starting point for extracting further information from log messages already ingested into Elasticsearch.
          For example, if you have ingested data into a very simple index that has just <code>@timestamp</code> and message fields, you can use this API to see what common structure exists in the message field.</p>
          <p>The response from the API contains:</p>
          <ul>
          <li>Sample messages.</li>
          <li>Statistics that reveal the most common values for all fields detected within the text and basic numeric statistics for numeric fields.</li>
          <li>Information about the structure of the text, which is useful when you write ingest configurations to index it or similarly formatted text.</li>
          <li>Appropriate mappings for an Elasticsearch index, which you could use to ingest the text.</li>
          </ul>
          <p>All this information can be calculated by the structure finder with no guidance.
          However, you can optionally override some of the decisions about the text structure by specifying one or more query parameters.</p>
          <p>If the structure finder produces unexpected results, specify the <code>explain</code> query parameter and an explanation will appear in the response.
          It helps determine why the returned structure was chosen.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-text_structure>`_

        :param field: The field that should be analyzed.
        :param index: The name of the index that contains the analyzed field.
        :param column_names: If `format` is set to `delimited`, you can specify the column
            names in a comma-separated list. If this parameter is not specified, the
            structure finder uses the column names from the header row of the text. If
            the text does not have a header row, columns are named "column1", "column2",
            "column3", for example.
        :param delimiter: If you have set `format` to `delimited`, you can specify the
            character used to delimit the values in each row. Only a single character
            is supported; the delimiter cannot have multiple characters. By default,
            the API considers the following possibilities: comma, tab, semi-colon, and
            pipe (`|`). In this default scenario, all rows must have the same number
            of fields for the delimited format to be detected. If you specify a delimiter,
            up to 10% of the rows can have a different number of columns than the first
            row.
        :param documents_to_sample: The number of documents to include in the structural
            analysis. The minimum value is 2.
        :param ecs_compatibility: The mode of compatibility with ECS compliant Grok patterns.
            Use this parameter to specify whether to use ECS Grok patterns instead of
            legacy ones when the structure finder creates a Grok pattern. This setting
            primarily has an impact when a whole message Grok pattern such as `%{CATALINALOG}`
            matches the input. If the structure finder identifies a common structure
            but has no idea of the meaning then generic field names such as `path`, `ipaddress`,
            `field1`, and `field2` are used in the `grok_pattern` output. The intention
            in that situation is that a user who knows the meanings will rename the fields
            before using them.
        :param explain: If `true`, the response includes a field named `explanation`,
            which is an array of strings that indicate how the structure finder produced
            its result.
        :param format: The high level structure of the text. By default, the API chooses
            the format. In this default scenario, all rows must have the same number
            of fields for a delimited format to be detected. If the format is set to
            delimited and the delimiter is not set, however, the API tolerates up to
            5% of rows that have a different number of columns than the first row.
        :param grok_pattern: If the format is `semi_structured_text`, you can specify
            a Grok pattern that is used to extract fields from every message in the text.
            The name of the timestamp field in the Grok pattern must match what is specified
            in the `timestamp_field` parameter. If that parameter is not specified, the
            name of the timestamp field in the Grok pattern must match "timestamp". If
            `grok_pattern` is not specified, the structure finder creates a Grok pattern.
        :param quote: If the format is `delimited`, you can specify the character used
            to quote the values in each row if they contain newlines or the delimiter
            character. Only a single character is supported. If this parameter is not
            specified, the default value is a double quote (`"`). If your delimited text
            format does not use quoting, a workaround is to set this argument to a character
            that does not appear anywhere in the sample.
        :param should_trim_fields: If the format is `delimited`, you can specify whether
            values between delimiters should have whitespace trimmed from them. If this
            parameter is not specified and the delimiter is pipe (`|`), the default value
            is true. Otherwise, the default value is `false`.
        :param timeout: The maximum amount of time that the structure analysis can take.
            If the analysis is still running when the timeout expires, it will be stopped.
        :param timestamp_field: The name of the field that contains the primary timestamp
            of each record in the text. In particular, if the text was ingested into
            an index, this is the field that would be used to populate the `@timestamp`
            field. If the format is `semi_structured_text`, this field must match the
            name of the appropriate extraction in the `grok_pattern`. Therefore, for
            semi-structured text, it is best not to specify this parameter unless `grok_pattern`
            is also specified. For structured text, if you specify this parameter, the
            field must exist within the text. If this parameter is not specified, the
            structure finder makes a decision about which field (if any) is the primary
            timestamp field. For structured text, it is not compulsory to have a timestamp
            in the text.
        :param timestamp_format: The Java time format of the timestamp field in the text.
            Only a subset of Java time format letter groups are supported: * `a` * `d`
            * `dd` * `EEE` * `EEEE` * `H` * `HH` * `h` * `M` * `MM` * `MMM` * `MMMM`
            * `mm` * `ss` * `XX` * `XXX` * `yy` * `yyyy` * `zzz` Additionally `S` letter
            groups (fractional seconds) of length one to nine are supported providing
            they occur after `ss` and are separated from the `ss` by a period (`.`),
            comma (`,`), or colon (`:`). Spacing and punctuation is also permitted with
            the exception a question mark (`?`), newline, and carriage return, together
            with literal text enclosed in single quotes. For example, `MM/dd HH.mm.ss,SSSSSS
            'in' yyyy` is a valid override format. One valuable use case for this parameter
            is when the format is semi-structured text, there are multiple timestamp
            formats in the text, and you know which format corresponds to the primary
            timestamp, but you do not want to specify the full `grok_pattern`. Another
            is when the timestamp format is one that the structure finder does not consider
            by default. If this parameter is not specified, the structure finder chooses
            the best format from a built-in set. If the special value `null` is specified,
            the structure finder will not look for a primary timestamp in the text. When
            the format is semi-structured text, this will result in the structure finder
            treating the text as single-line messages.
        """
        if field is None:
            raise ValueError("Empty value passed for parameter 'field'")
        if index is None:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_text_structure/find_field_structure"
        __query: t.Dict[str, t.Any] = {}
        if field is not None:
            __query["field"] = field
        if index is not None:
            __query["index"] = index
        if column_names is not None:
            __query["column_names"] = column_names
        if delimiter is not None:
            __query["delimiter"] = delimiter
        if documents_to_sample is not None:
            __query["documents_to_sample"] = documents_to_sample
        if ecs_compatibility is not None:
            __query["ecs_compatibility"] = ecs_compatibility
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if explain is not None:
            __query["explain"] = explain
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if format is not None:
            __query["format"] = format
        if grok_pattern is not None:
            __query["grok_pattern"] = grok_pattern
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if quote is not None:
            __query["quote"] = quote
        if should_trim_fields is not None:
            __query["should_trim_fields"] = should_trim_fields
        if timeout is not None:
            __query["timeout"] = timeout
        if timestamp_field is not None:
            __query["timestamp_field"] = timestamp_field
        if timestamp_format is not None:
            __query["timestamp_format"] = timestamp_format
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="text_structure.find_field_structure",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("messages",),
    )
    async def find_message_structure(
        self,
        *,
        messages: t.Optional[t.Sequence[str]] = None,
        column_names: t.Optional[str] = None,
        delimiter: t.Optional[str] = None,
        ecs_compatibility: t.Optional[t.Union[str, t.Literal["disabled", "v1"]]] = None,
        error_trace: t.Optional[bool] = None,
        explain: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[
            t.Union[
                str, t.Literal["delimited", "ndjson", "semi_structured_text", "xml"]
            ]
        ] = None,
        grok_pattern: t.Optional[str] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        quote: t.Optional[str] = None,
        should_trim_fields: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        timestamp_field: t.Optional[str] = None,
        timestamp_format: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Find the structure of text messages.
          Find the structure of a list of text messages.
          The messages must contain data that is suitable to be ingested into Elasticsearch.</p>
          <p>This API provides a starting point for ingesting data into Elasticsearch in a format that is suitable for subsequent use with other Elastic Stack functionality.
          Use this API rather than the find text structure API if your input text has already been split up into separate messages by some other process.</p>
          <p>The response from the API contains:</p>
          <ul>
          <li>Sample messages.</li>
          <li>Statistics that reveal the most common values for all fields detected within the text and basic numeric statistics for numeric fields.</li>
          <li>Information about the structure of the text, which is useful when you write ingest configurations to index it or similarly formatted text.
          Appropriate mappings for an Elasticsearch index, which you could use to ingest the text.</li>
          </ul>
          <p>All this information can be calculated by the structure finder with no guidance.
          However, you can optionally override some of the decisions about the text structure by specifying one or more query parameters.</p>
          <p>If the structure finder produces unexpected results, specify the <code>explain</code> query parameter and an explanation will appear in the response.
          It helps determine why the returned structure was chosen.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-message-structure>`_

        :param messages: The list of messages you want to analyze.
        :param column_names: If the format is `delimited`, you can specify the column
            names in a comma-separated list. If this parameter is not specified, the
            structure finder uses the column names from the header row of the text. If
            the text does not have a header role, columns are named "column1", "column2",
            "column3", for example.
        :param delimiter: If you the format is `delimited`, you can specify the character
            used to delimit the values in each row. Only a single character is supported;
            the delimiter cannot have multiple characters. By default, the API considers
            the following possibilities: comma, tab, semi-colon, and pipe (`|`). In this
            default scenario, all rows must have the same number of fields for the delimited
            format to be detected. If you specify a delimiter, up to 10% of the rows
            can have a different number of columns than the first row.
        :param ecs_compatibility: The mode of compatibility with ECS compliant Grok patterns.
            Use this parameter to specify whether to use ECS Grok patterns instead of
            legacy ones when the structure finder creates a Grok pattern. This setting
            primarily has an impact when a whole message Grok pattern such as `%{CATALINALOG}`
            matches the input. If the structure finder identifies a common structure
            but has no idea of meaning then generic field names such as `path`, `ipaddress`,
            `field1`, and `field2` are used in the `grok_pattern` output, with the intention
            that a user who knows the meanings rename these fields before using it.
        :param explain: If this parameter is set to true, the response includes a field
            named `explanation`, which is an array of strings that indicate how the structure
            finder produced its result.
        :param format: The high level structure of the text. By default, the API chooses
            the format. In this default scenario, all rows must have the same number
            of fields for a delimited format to be detected. If the format is `delimited`
            and the delimiter is not set, however, the API tolerates up to 5% of rows
            that have a different number of columns than the first row.
        :param grok_pattern: If the format is `semi_structured_text`, you can specify
            a Grok pattern that is used to extract fields from every message in the text.
            The name of the timestamp field in the Grok pattern must match what is specified
            in the `timestamp_field` parameter. If that parameter is not specified, the
            name of the timestamp field in the Grok pattern must match "timestamp". If
            `grok_pattern` is not specified, the structure finder creates a Grok pattern.
        :param quote: If the format is `delimited`, you can specify the character used
            to quote the values in each row if they contain newlines or the delimiter
            character. Only a single character is supported. If this parameter is not
            specified, the default value is a double quote (`"`). If your delimited text
            format does not use quoting, a workaround is to set this argument to a character
            that does not appear anywhere in the sample.
        :param should_trim_fields: If the format is `delimited`, you can specify whether
            values between delimiters should have whitespace trimmed from them. If this
            parameter is not specified and the delimiter is pipe (`|`), the default value
            is true. Otherwise, the default value is `false`.
        :param timeout: The maximum amount of time that the structure analysis can take.
            If the analysis is still running when the timeout expires, it will be stopped.
        :param timestamp_field: The name of the field that contains the primary timestamp
            of each record in the text. In particular, if the text was ingested into
            an index, this is the field that would be used to populate the `@timestamp`
            field. If the format is `semi_structured_text`, this field must match the
            name of the appropriate extraction in the `grok_pattern`. Therefore, for
            semi-structured text, it is best not to specify this parameter unless `grok_pattern`
            is also specified. For structured text, if you specify this parameter, the
            field must exist within the text. If this parameter is not specified, the
            structure finder makes a decision about which field (if any) is the primary
            timestamp field. For structured text, it is not compulsory to have a timestamp
            in the text.
        :param timestamp_format: The Java time format of the timestamp field in the text.
            Only a subset of Java time format letter groups are supported: * `a` * `d`
            * `dd` * `EEE` * `EEEE` * `H` * `HH` * `h` * `M` * `MM` * `MMM` * `MMMM`
            * `mm` * `ss` * `XX` * `XXX` * `yy` * `yyyy` * `zzz` Additionally `S` letter
            groups (fractional seconds) of length one to nine are supported providing
            they occur after `ss` and are separated from the `ss` by a period (`.`),
            comma (`,`), or colon (`:`). Spacing and punctuation is also permitted with
            the exception a question mark (`?`), newline, and carriage return, together
            with literal text enclosed in single quotes. For example, `MM/dd HH.mm.ss,SSSSSS
            'in' yyyy` is a valid override format. One valuable use case for this parameter
            is when the format is semi-structured text, there are multiple timestamp
            formats in the text, and you know which format corresponds to the primary
            timestamp, but you do not want to specify the full `grok_pattern`. Another
            is when the timestamp format is one that the structure finder does not consider
            by default. If this parameter is not specified, the structure finder chooses
            the best format from a built-in set. If the special value `null` is specified,
            the structure finder will not look for a primary timestamp in the text. When
            the format is semi-structured text, this will result in the structure finder
            treating the text as single-line messages.
        """
        if messages is None and body is None:
            raise ValueError("Empty value passed for parameter 'messages'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_text_structure/find_message_structure"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if column_names is not None:
            __query["column_names"] = column_names
        if delimiter is not None:
            __query["delimiter"] = delimiter
        if ecs_compatibility is not None:
            __query["ecs_compatibility"] = ecs_compatibility
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if explain is not None:
            __query["explain"] = explain
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if format is not None:
            __query["format"] = format
        if grok_pattern is not None:
            __query["grok_pattern"] = grok_pattern
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if quote is not None:
            __query["quote"] = quote
        if should_trim_fields is not None:
            __query["should_trim_fields"] = should_trim_fields
        if timeout is not None:
            __query["timeout"] = timeout
        if timestamp_field is not None:
            __query["timestamp_field"] = timestamp_field
        if timestamp_format is not None:
            __query["timestamp_format"] = timestamp_format
        if not __body:
            if messages is not None:
                __body["messages"] = messages
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="text_structure.find_message_structure",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="text_files",
    )
    async def find_structure(
        self,
        *,
        text_files: t.Optional[t.Sequence[t.Any]] = None,
        body: t.Optional[t.Sequence[t.Any]] = None,
        charset: t.Optional[str] = None,
        column_names: t.Optional[str] = None,
        delimiter: t.Optional[str] = None,
        ecs_compatibility: t.Optional[str] = None,
        explain: t.Optional[bool] = None,
        format: t.Optional[str] = None,
        grok_pattern: t.Optional[str] = None,
        has_header_row: t.Optional[bool] = None,
        line_merge_size_limit: t.Optional[int] = None,
        lines_to_sample: t.Optional[int] = None,
        quote: t.Optional[str] = None,
        should_trim_fields: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        timestamp_field: t.Optional[str] = None,
        timestamp_format: t.Optional[str] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Find the structure of a text file.
          The text file must contain data that is suitable to be ingested into Elasticsearch.</p>
          <p>This API provides a starting point for ingesting data into Elasticsearch in a format that is suitable for subsequent use with other Elastic Stack functionality.
          Unlike other Elasticsearch endpoints, the data that is posted to this endpoint does not need to be UTF-8 encoded and in JSON format.
          It must, however, be text; binary text formats are not currently supported.
          The size is limited to the Elasticsearch HTTP receive buffer size, which defaults to 100 Mb.</p>
          <p>The response from the API contains:</p>
          <ul>
          <li>A couple of messages from the beginning of the text.</li>
          <li>Statistics that reveal the most common values for all fields detected within the text and basic numeric statistics for numeric fields.</li>
          <li>Information about the structure of the text, which is useful when you write ingest configurations to index it or similarly formatted text.</li>
          <li>Appropriate mappings for an Elasticsearch index, which you could use to ingest the text.</li>
          </ul>
          <p>All this information can be calculated by the structure finder with no guidance.
          However, you can optionally override some of the decisions about the text structure by specifying one or more query parameters.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-structure>`_

        :param text_files:
        :param charset: The text's character set. It must be a character set that is
            supported by the JVM that Elasticsearch uses. For example, `UTF-8`, `UTF-16LE`,
            `windows-1252`, or `EUC-JP`. If this parameter is not specified, the structure
            finder chooses an appropriate character set.
        :param column_names: If you have set format to `delimited`, you can specify the
            column names in a comma-separated list. If this parameter is not specified,
            the structure finder uses the column names from the header row of the text.
            If the text does not have a header role, columns are named "column1", "column2",
            "column3", for example.
        :param delimiter: If you have set `format` to `delimited`, you can specify the
            character used to delimit the values in each row. Only a single character
            is supported; the delimiter cannot have multiple characters. By default,
            the API considers the following possibilities: comma, tab, semi-colon, and
            pipe (`|`). In this default scenario, all rows must have the same number
            of fields for the delimited format to be detected. If you specify a delimiter,
            up to 10% of the rows can have a different number of columns than the first
            row.
        :param ecs_compatibility: The mode of compatibility with ECS compliant Grok patterns.
            Use this parameter to specify whether to use ECS Grok patterns instead of
            legacy ones when the structure finder creates a Grok pattern. Valid values
            are `disabled` and `v1`. This setting primarily has an impact when a whole
            message Grok pattern such as `%{CATALINALOG}` matches the input. If the structure
            finder identifies a common structure but has no idea of meaning then generic
            field names such as `path`, `ipaddress`, `field1`, and `field2` are used
            in the `grok_pattern` output, with the intention that a user who knows the
            meanings rename these fields before using it.
        :param explain: If this parameter is set to `true`, the response includes a field
            named explanation, which is an array of strings that indicate how the structure
            finder produced its result. If the structure finder produces unexpected results
            for some text, use this query parameter to help you determine why the returned
            structure was chosen.
        :param format: The high level structure of the text. Valid values are `ndjson`,
            `xml`, `delimited`, and `semi_structured_text`. By default, the API chooses
            the format. In this default scenario, all rows must have the same number
            of fields for a delimited format to be detected. If the format is set to
            `delimited` and the delimiter is not set, however, the API tolerates up to
            5% of rows that have a different number of columns than the first row.
        :param grok_pattern: If you have set `format` to `semi_structured_text`, you
            can specify a Grok pattern that is used to extract fields from every message
            in the text. The name of the timestamp field in the Grok pattern must match
            what is specified in the `timestamp_field` parameter. If that parameter is
            not specified, the name of the timestamp field in the Grok pattern must match
            "timestamp". If `grok_pattern` is not specified, the structure finder creates
            a Grok pattern.
        :param has_header_row: If you have set `format` to `delimited`, you can use this
            parameter to indicate whether the column names are in the first row of the
            text. If this parameter is not specified, the structure finder guesses based
            on the similarity of the first row of the text to other rows.
        :param line_merge_size_limit: The maximum number of characters in a message when
            lines are merged to form messages while analyzing semi-structured text. If
            you have extremely long messages you may need to increase this, but be aware
            that this may lead to very long processing times if the way to group lines
            into messages is misdetected.
        :param lines_to_sample: The number of lines to include in the structural analysis,
            starting from the beginning of the text. The minimum is 2. If the value of
            this parameter is greater than the number of lines in the text, the analysis
            proceeds (as long as there are at least two lines in the text) for all of
            the lines. NOTE: The number of lines and the variation of the lines affects
            the speed of the analysis. For example, if you upload text where the first
            1000 lines are all variations on the same message, the analysis will find
            more commonality than would be seen with a bigger sample. If possible, however,
            it is more efficient to upload sample text with more variety in the first
            1000 lines than to request analysis of 100000 lines to achieve some variety.
        :param quote: If you have set `format` to `delimited`, you can specify the character
            used to quote the values in each row if they contain newlines or the delimiter
            character. Only a single character is supported. If this parameter is not
            specified, the default value is a double quote (`"`). If your delimited text
            format does not use quoting, a workaround is to set this argument to a character
            that does not appear anywhere in the sample.
        :param should_trim_fields: If you have set `format` to `delimited`, you can specify
            whether values between delimiters should have whitespace trimmed from them.
            If this parameter is not specified and the delimiter is pipe (`|`), the default
            value is `true`. Otherwise, the default value is `false`.
        :param timeout: The maximum amount of time that the structure analysis can take.
            If the analysis is still running when the timeout expires then it will be
            stopped.
        :param timestamp_field: The name of the field that contains the primary timestamp
            of each record in the text. In particular, if the text were ingested into
            an index, this is the field that would be used to populate the `@timestamp`
            field. If the `format` is `semi_structured_text`, this field must match the
            name of the appropriate extraction in the `grok_pattern`. Therefore, for
            semi-structured text, it is best not to specify this parameter unless `grok_pattern`
            is also specified. For structured text, if you specify this parameter, the
            field must exist within the text. If this parameter is not specified, the
            structure finder makes a decision about which field (if any) is the primary
            timestamp field. For structured text, it is not compulsory to have a timestamp
            in the text.
        :param timestamp_format: The Java time format of the timestamp field in the text.
            Only a subset of Java time format letter groups are supported: * `a` * `d`
            * `dd` * `EEE` * `EEEE` * `H` * `HH` * `h` * `M` * `MM` * `MMM` * `MMMM`
            * `mm` * `ss` * `XX` * `XXX` * `yy` * `yyyy` * `zzz` Additionally `S` letter
            groups (fractional seconds) of length one to nine are supported providing
            they occur after `ss` and separated from the `ss` by a `.`, `,` or `:`. Spacing
            and punctuation is also permitted with the exception of `?`, newline and
            carriage return, together with literal text enclosed in single quotes. For
            example, `MM/dd HH.mm.ss,SSSSSS 'in' yyyy` is a valid override format. One
            valuable use case for this parameter is when the format is semi-structured
            text, there are multiple timestamp formats in the text, and you know which
            format corresponds to the primary timestamp, but you do not want to specify
            the full `grok_pattern`. Another is when the timestamp format is one that
            the structure finder does not consider by default. If this parameter is not
            specified, the structure finder chooses the best format from a built-in set.
            If the special value `null` is specified the structure finder will not look
            for a primary timestamp in the text. When the format is semi-structured text
            this will result in the structure finder treating the text as single-line
            messages.
        """
        if text_files is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'text_files' and 'body', one of them should be set."
            )
        elif text_files is not None and body is not None:
            raise ValueError("Cannot set both 'text_files' and 'body'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_text_structure/find_structure"
        __query: t.Dict[str, t.Any] = {}
        if charset is not None:
            __query["charset"] = charset
        if column_names is not None:
            __query["column_names"] = column_names
        if delimiter is not None:
            __query["delimiter"] = delimiter
        if ecs_compatibility is not None:
            __query["ecs_compatibility"] = ecs_compatibility
        if explain is not None:
            __query["explain"] = explain
        if format is not None:
            __query["format"] = format
        if grok_pattern is not None:
            __query["grok_pattern"] = grok_pattern
        if has_header_row is not None:
            __query["has_header_row"] = has_header_row
        if line_merge_size_limit is not None:
            __query["line_merge_size_limit"] = line_merge_size_limit
        if lines_to_sample is not None:
            __query["lines_to_sample"] = lines_to_sample
        if quote is not None:
            __query["quote"] = quote
        if should_trim_fields is not None:
            __query["should_trim_fields"] = should_trim_fields
        if timeout is not None:
            __query["timeout"] = timeout
        if timestamp_field is not None:
            __query["timestamp_field"] = timestamp_field
        if timestamp_format is not None:
            __query["timestamp_format"] = timestamp_format
        __body = text_files if text_files is not None else body
        __headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="text_structure.find_structure",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("grok_pattern", "text"),
    )
    async def test_grok_pattern(
        self,
        *,
        grok_pattern: t.Optional[str] = None,
        text: t.Optional[t.Sequence[str]] = None,
        ecs_compatibility: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Test a Grok pattern.
          Test a Grok pattern on one or more lines of text.
          The API indicates whether the lines match the pattern together with the offsets and lengths of the matched substrings.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-test-grok-pattern>`_

        :param grok_pattern: The Grok pattern to run on the text.
        :param text: The lines of text to run the Grok pattern on.
        :param ecs_compatibility: The mode of compatibility with ECS compliant Grok patterns.
            Use this parameter to specify whether to use ECS Grok patterns instead of
            legacy ones when the structure finder creates a Grok pattern. Valid values
            are `disabled` and `v1`.
        """
        if grok_pattern is None and body is None:
            raise ValueError("Empty value passed for parameter 'grok_pattern'")
        if text is None and body is None:
            raise ValueError("Empty value passed for parameter 'text'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_text_structure/test_grok_pattern"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if ecs_compatibility is not None:
            __query["ecs_compatibility"] = ecs_compatibility
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if grok_pattern is not None:
                __body["grok_pattern"] = grok_pattern
            if text is not None:
                __body["text"] = text
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="text_structure.test_grok_pattern",
            path_parts=__path_parts,
        )
