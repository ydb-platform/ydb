# Copyright The OpenTelemetry Authors
#
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

import abc
import inspect
from typing import Any, Dict, MutableMapping, Optional, Tuple

from opentelemetry.instrumentation.botocore.extensions._messaging import (
    inject_propagation_context,
)
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkCallContext,
    _AwsSdkExtension,
    _BotocoreInstrumentorContext,
    _BotoResultT,
)
from opentelemetry.semconv._incubating.attributes.aws_attributes import (
    AWS_SNS_TOPIC_ARN,
)
from opentelemetry.semconv.trace import (
    MessagingDestinationKindValues,
    SpanAttributes,
)
from opentelemetry.trace import SpanKind
from opentelemetry.trace.span import Span

################################################################################
# SNS operations
################################################################################


class _SnsOperation(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def operation_name(cls) -> str:
        pass

    @classmethod
    def span_kind(cls) -> SpanKind:
        return SpanKind.CLIENT

    @classmethod
    def extract_attributes(
        cls, call_context: _AwsSdkCallContext, attributes: _AttributeMapT
    ):
        pass

    @classmethod
    def before_service_call(cls, call_context: _AwsSdkCallContext, span: Span):
        pass


class _OpPublish(_SnsOperation):
    _arn_arg_names = ("TopicArn", "TargetArn")
    _phone_arg_name = "PhoneNumber"

    @classmethod
    def operation_name(cls) -> str:
        return "Publish"

    @classmethod
    def span_kind(cls) -> SpanKind:
        return SpanKind.PRODUCER

    @classmethod
    def extract_attributes(
        cls, call_context: _AwsSdkCallContext, attributes: _AttributeMapT
    ):
        span_name, destination_name = cls._extract_destination_name(
            call_context
        )

        call_context.span_name = f"{span_name} send"

        attributes[SpanAttributes.MESSAGING_DESTINATION_KIND] = (
            MessagingDestinationKindValues.TOPIC.value
        )
        attributes[SpanAttributes.MESSAGING_DESTINATION] = destination_name
        attributes[SpanAttributes.MESSAGING_DESTINATION_NAME] = (
            destination_name
        )

    @classmethod
    def _extract_destination_name(
        cls, call_context: _AwsSdkCallContext
    ) -> Tuple[str, str]:
        arn = cls._extract_input_arn(call_context)
        if arn:
            return arn.rsplit(":", 1)[-1], arn

        if cls._phone_arg_name:
            phone_number = call_context.params.get(cls._phone_arg_name)
            if phone_number:
                # phone number redacted because it's a PII
                return "phone_number", "phone_number:**"

        return "unknown", "unknown"

    @classmethod
    def _extract_input_arn(
        cls, call_context: _AwsSdkCallContext
    ) -> Optional[str]:
        for input_arn in cls._arn_arg_names:
            arn = call_context.params.get(input_arn)
            if arn:
                return arn
        return None

    @classmethod
    def before_service_call(cls, call_context: _AwsSdkCallContext, span: Span):
        cls._inject_span_into_entry(call_context.params)

    @classmethod
    def _inject_span_into_entry(cls, entry: MutableMapping[str, Any]):
        entry["MessageAttributes"] = inject_propagation_context(
            entry.get("MessageAttributes")
        )


class _OpPublishBatch(_OpPublish):
    _arn_arg_names = ("TopicArn",)
    _phone_arg_name = None

    @classmethod
    def operation_name(cls) -> str:
        return "PublishBatch"

    @classmethod
    def before_service_call(cls, call_context: _AwsSdkCallContext, span: Span):
        for entry in call_context.params.get("PublishBatchRequestEntries", ()):
            cls._inject_span_into_entry(entry)


################################################################################
# SNS extension
################################################################################

_OPERATION_MAPPING: Dict[str, _SnsOperation] = {
    op.operation_name(): op
    for op in globals().values()
    if inspect.isclass(op)
    and issubclass(op, _SnsOperation)
    and not inspect.isabstract(op)
}


class _SnsExtension(_AwsSdkExtension):
    def __init__(self, call_context: _AwsSdkCallContext):
        super().__init__(call_context)
        self._op = _OPERATION_MAPPING.get(call_context.operation)
        if self._op:
            call_context.span_kind = self._op.span_kind()

    def extract_attributes(self, attributes: _AttributeMapT):
        attributes[SpanAttributes.MESSAGING_SYSTEM] = "aws.sns"
        topic_arn = self._call_context.params.get("TopicArn")
        if topic_arn:
            attributes[AWS_SNS_TOPIC_ARN] = topic_arn

        if self._op:
            self._op.extract_attributes(self._call_context, attributes)

    def before_service_call(
        self, span: Span, instrumentor_context: _BotocoreInstrumentorContext
    ):
        if self._op:
            self._op.before_service_call(self._call_context, span)

    def on_success(
        self,
        span: Span,
        result: _BotoResultT,
        instrumentor_context: _BotocoreInstrumentorContext,
    ):
        if not span.is_recording():
            return

        topic_arn = result.get("TopicArn")
        if topic_arn:
            span.set_attribute(AWS_SNS_TOPIC_ARN, topic_arn)
