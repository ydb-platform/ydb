# Copyright (c) 2019 The Jaeger Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
from jaeger_client import Span, SpanContext


def test_traceback():
    """Test that a traceback is logged with both location and message"""
    mock_tracer = mock.MagicMock()
    mock_tracer.max_tag_value_length = 300
    mock_tracer.max_traceback_length = 300
    context = SpanContext(trace_id=1, span_id=2, parent_id=None, flags=1)
    span = Span(context=context, operation_name='traceback_test', tracer=mock_tracer)

    try:
        with span:
            raise ValueError('Something unexpected happened!')
    except ValueError:
        fields_dict = {field.key: field.vStr for field in span.logs[0].fields}
        assert 'stack' in fields_dict
        stack_message = fields_dict['stack']
        stack_message_lines = stack_message.splitlines()
        assert len(stack_message_lines) == 2
        assert stack_message_lines[0].startswith('  File ')
        assert stack_message_lines[1] == \
            "    raise ValueError('Something unexpected happened!')"


def test_traceback_cut():
    """Test that a traceback is cut off at max_tag_value_length"""
    mock_tracer = mock.MagicMock()
    mock_tracer.max_tag_value_length = 300
    mock_tracer.max_traceback_length = 5
    context = SpanContext(trace_id=1, span_id=2, parent_id=None, flags=1)
    span = Span(context=context, operation_name='traceback_test', tracer=mock_tracer)

    try:
        with span:
            raise ValueError('Something unexpected happened!')
    except ValueError:
        fields_dict = {field.key: field.vStr for field in span.logs[0].fields}
        assert 'stack' in fields_dict
        stack_message = fields_dict['stack']
        assert stack_message == '  Fil'
