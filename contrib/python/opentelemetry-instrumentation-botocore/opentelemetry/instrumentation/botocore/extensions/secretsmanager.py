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
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkExtension,
    _BotocoreInstrumentorContext,
    _BotoResultT,
)
from opentelemetry.semconv._incubating.attributes.aws_attributes import (
    AWS_SECRETSMANAGER_SECRET_ARN,
)
from opentelemetry.trace.span import Span


class _SecretsManagerExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        """
        SecretId is extracted if a secret ARN, the function extracts the attribute
        only if the SecretId parameter is provided as an arn which starts with
        `arn:aws:secretsmanager:`
        """
        secret_id = self._call_context.params.get("SecretId")
        if secret_id and secret_id.startswith("arn:aws:secretsmanager:"):
            attributes[AWS_SECRETSMANAGER_SECRET_ARN] = secret_id

    def on_success(
        self,
        span: Span,
        result: _BotoResultT,
        instrumentor_context: _BotocoreInstrumentorContext,
    ):
        secret_arn = result.get("ARN")
        if secret_arn:
            span.set_attribute(AWS_SECRETSMANAGER_SECRET_ARN, secret_arn)
