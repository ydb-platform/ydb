# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

GEN_AI_AGENT_DESCRIPTION: Final = "gen_ai.agent.description"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_AGENT_ID: Final = "gen_ai.agent.id"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_AGENT_NAME: Final = "gen_ai.agent.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_AGENT_VERSION: Final = "gen_ai.agent.version"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_COMPLETION: Final = "gen_ai.completion"
"""
Deprecated: Removed, no replacement at this time.
"""

GEN_AI_CONVERSATION_ID: Final = "gen_ai.conversation.id"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_DATA_SOURCE_ID: Final = "gen_ai.data_source.id"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_EMBEDDINGS_DIMENSION_COUNT: Final = "gen_ai.embeddings.dimension.count"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_EVALUATION_EXPLANATION: Final = "gen_ai.evaluation.explanation"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_EVALUATION_NAME: Final = "gen_ai.evaluation.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_EVALUATION_SCORE_LABEL: Final = "gen_ai.evaluation.score.label"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_EVALUATION_SCORE_VALUE: Final = "gen_ai.evaluation.score.value"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_INPUT_MESSAGES: Final = "gen_ai.input.messages"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OPENAI_REQUEST_RESPONSE_FORMAT: Final = (
    "gen_ai.openai.request.response_format"
)
"""
Deprecated: Replaced by `gen_ai.output.type`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OPENAI_REQUEST_SEED: Final = "gen_ai.openai.request.seed"
"""
Deprecated: Replaced by `gen_ai.request.seed`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OPENAI_REQUEST_SERVICE_TIER: Final = (
    "gen_ai.openai.request.service_tier"
)
"""
Deprecated: Replaced by `openai.request.service_tier`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OPENAI_RESPONSE_SERVICE_TIER: Final = (
    "gen_ai.openai.response.service_tier"
)
"""
Deprecated: Replaced by `openai.response.service_tier`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT: Final = (
    "gen_ai.openai.response.system_fingerprint"
)
"""
Deprecated: Replaced by `openai.response.system_fingerprint`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OPERATION_NAME: Final = "gen_ai.operation.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OUTPUT_MESSAGES: Final = "gen_ai.output.messages"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_OUTPUT_TYPE: Final = "gen_ai.output.type"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_PROMPT: Final = "gen_ai.prompt"
"""
Deprecated: Removed, no replacement at this time.
"""

GEN_AI_PROMPT_NAME: Final = "gen_ai.prompt.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_PROVIDER_NAME: Final = "gen_ai.provider.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_CHOICE_COUNT: Final = "gen_ai.request.choice.count"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_ENCODING_FORMATS: Final = "gen_ai.request.encoding_formats"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_FREQUENCY_PENALTY: Final = "gen_ai.request.frequency_penalty"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_MAX_TOKENS: Final = "gen_ai.request.max_tokens"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_MODEL: Final = "gen_ai.request.model"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_PRESENCE_PENALTY: Final = "gen_ai.request.presence_penalty"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_SEED: Final = "gen_ai.request.seed"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_STOP_SEQUENCES: Final = "gen_ai.request.stop_sequences"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_STREAM: Final = "gen_ai.request.stream"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_TEMPERATURE: Final = "gen_ai.request.temperature"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_TOP_K: Final = "gen_ai.request.top_k"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_REQUEST_TOP_P: Final = "gen_ai.request.top_p"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_RESPONSE_FINISH_REASONS: Final = "gen_ai.response.finish_reasons"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_RESPONSE_ID: Final = "gen_ai.response.id"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_RESPONSE_MODEL: Final = "gen_ai.response.model"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_RESPONSE_TIME_TO_FIRST_CHUNK: Final = (
    "gen_ai.response.time_to_first_chunk"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_RETRIEVAL_DOCUMENTS: Final = "gen_ai.retrieval.documents"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_RETRIEVAL_QUERY_TEXT: Final = "gen_ai.retrieval.query.text"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_SYSTEM: Final = "gen_ai.system"
"""
Deprecated: Replaced by `gen_ai.provider.name`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_SYSTEM_INSTRUCTIONS: Final = "gen_ai.system_instructions"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOKEN_TYPE: Final = "gen_ai.token.type"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_CALL_ARGUMENTS: Final = "gen_ai.tool.call.arguments"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_CALL_ID: Final = "gen_ai.tool.call.id"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_CALL_RESULT: Final = "gen_ai.tool.call.result"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_DEFINITIONS: Final = "gen_ai.tool.definitions"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_DESCRIPTION: Final = "gen_ai.tool.description"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_NAME: Final = "gen_ai.tool.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_TOOL_TYPE: Final = "gen_ai.tool.type"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS: Final = (
    "gen_ai.usage.cache_creation.input_tokens"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS: Final = (
    "gen_ai.usage.cache_read.input_tokens"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_COMPLETION_TOKENS: Final = "gen_ai.usage.completion_tokens"
"""
Deprecated: Replaced by `gen_ai.usage.output_tokens`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_INPUT_TOKENS: Final = "gen_ai.usage.input_tokens"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_OUTPUT_TOKENS: Final = "gen_ai.usage.output_tokens"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_PROMPT_TOKENS: Final = "gen_ai.usage.prompt_tokens"
"""
Deprecated: Replaced by `gen_ai.usage.input_tokens`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_USAGE_REASONING_OUTPUT_TOKENS: Final = (
    "gen_ai.usage.reasoning.output_tokens"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

GEN_AI_WORKFLOW_NAME: Final = "gen_ai.workflow.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


@deprecated(
    "The attribute gen_ai.openai.request.response_format is deprecated - Replaced by `gen_ai.output.type`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiOpenaiRequestResponseFormatValues(Enum):
    TEXT = "text"
    """Text response format."""
    JSON_OBJECT = "json_object"
    """JSON object response format."""
    JSON_SCHEMA = "json_schema"
    """JSON schema response format."""


@deprecated(
    "The attribute gen_ai.openai.request.service_tier is deprecated - Replaced by `openai.request.service_tier`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiOpenaiRequestServiceTierValues(Enum):
    AUTO = "auto"
    """The system will utilize scale tier credits until they are exhausted."""
    DEFAULT = "default"
    """The system will utilize the default scale tier."""


@deprecated(
    "The attribute gen_ai.operation.name is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiOperationNameValues(Enum):
    CHAT = "chat"
    """Chat completion operation such as [OpenAI Chat API](https://platform.openai.com/docs/api-reference/chat)."""
    GENERATE_CONTENT = "generate_content"
    """Multimodal content generation operation such as [Gemini Generate Content](https://ai.google.dev/api/generate-content)."""
    TEXT_COMPLETION = "text_completion"
    """Text completions operation such as [OpenAI Completions API (Legacy)](https://platform.openai.com/docs/api-reference/completions)."""
    EMBEDDINGS = "embeddings"
    """Embeddings operation such as [OpenAI Create embeddings API](https://platform.openai.com/docs/api-reference/embeddings/create)."""
    RETRIEVAL = "retrieval"
    """Retrieval operation such as [OpenAI Search Vector Store API](https://platform.openai.com/docs/api-reference/vector-stores/search)."""
    CREATE_AGENT = "create_agent"
    """Create GenAI agent."""
    INVOKE_AGENT = "invoke_agent"
    """Invoke GenAI agent."""
    EXECUTE_TOOL = "execute_tool"
    """Execute a tool."""
    INVOKE_WORKFLOW = "invoke_workflow"
    """Invoke GenAI workflow."""


@deprecated(
    "The attribute gen_ai.output.type is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiOutputTypeValues(Enum):
    TEXT = "text"
    """Plain text."""
    JSON = "json"
    """JSON object with known or unknown schema."""
    IMAGE = "image"
    """Image."""
    SPEECH = "speech"
    """Speech."""


@deprecated(
    "The attribute gen_ai.provider.name is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiProviderNameValues(Enum):
    OPENAI = "openai"
    """[OpenAI](https://openai.com/)."""
    GCP_GEN_AI = "gcp.gen_ai"
    """Any Google generative AI endpoint."""
    GCP_VERTEX_AI = "gcp.vertex_ai"
    """[Vertex AI](https://cloud.google.com/vertex-ai)."""
    GCP_GEMINI = "gcp.gemini"
    """[Gemini](https://cloud.google.com/products/gemini)."""
    ANTHROPIC = "anthropic"
    """[Anthropic](https://www.anthropic.com/)."""
    COHERE = "cohere"
    """[Cohere](https://cohere.com/)."""
    AZURE_AI_INFERENCE = "azure.ai.inference"
    """Azure AI Inference."""
    AZURE_AI_OPENAI = "azure.ai.openai"
    """[Azure OpenAI](https://learn.microsoft.com/en-us/azure/ai-services/openai/overview)."""
    IBM_WATSONX_AI = "ibm.watsonx.ai"
    """[IBM Watsonx AI](https://www.ibm.com/products/watsonx-ai)."""
    AWS_BEDROCK = "aws.bedrock"
    """[AWS Bedrock](https://aws.amazon.com/bedrock)."""
    PERPLEXITY = "perplexity"
    """[Perplexity](https://www.perplexity.ai/)."""
    X_AI = "x_ai"
    """[xAI](https://x.ai/)."""
    DEEPSEEK = "deepseek"
    """[DeepSeek](https://www.deepseek.com/)."""
    GROQ = "groq"
    """[Groq](https://groq.com/)."""
    MISTRAL_AI = "mistral_ai"
    """[Mistral AI](https://mistral.ai/)."""


@deprecated(
    "The attribute gen_ai.system is deprecated - Replaced by `gen_ai.provider.name`, which has moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiSystemValues(Enum):
    OPENAI = "openai"
    """OpenAI."""
    GCP_GEN_AI = "gcp.gen_ai"
    """Any Google generative AI endpoint."""
    GCP_VERTEX_AI = "gcp.vertex_ai"
    """Vertex AI."""
    GCP_GEMINI = "gcp.gemini"
    """Gemini."""
    VERTEX_AI = "vertex_ai"
    """Deprecated: Replaced by `gcp.vertex_ai`."""
    GEMINI = "gemini"
    """Deprecated: Replaced by `gcp.gemini`."""
    ANTHROPIC = "anthropic"
    """Anthropic."""
    COHERE = "cohere"
    """Cohere."""
    AZ_AI_INFERENCE = "az.ai.inference"
    """Deprecated: Replaced by `azure.ai.inference`."""
    AZ_AI_OPENAI = "az.ai.openai"
    """Deprecated: Replaced by `azure.ai.openai`."""
    AZURE_AI_INFERENCE = "azure.ai.inference"
    """Azure AI Inference."""
    AZURE_AI_OPENAI = "azure.ai.openai"
    """Azure OpenAI."""
    IBM_WATSONX_AI = "ibm.watsonx.ai"
    """IBM Watsonx AI."""
    AWS_BEDROCK = "aws.bedrock"
    """AWS Bedrock."""
    PERPLEXITY = "perplexity"
    """Perplexity."""
    XAI = "xai"
    """xAI."""
    DEEPSEEK = "deepseek"
    """DeepSeek."""
    GROQ = "groq"
    """Groq."""
    MISTRAL_AI = "mistral_ai"
    """Mistral AI."""


@deprecated(
    "The attribute gen_ai.token.type is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class GenAiTokenTypeValues(Enum):
    INPUT = "input"
    """Input tokens (prompt, input, etc.)."""
    COMPLETION = "output"
    """Deprecated: Replaced by `output`."""
    OUTPUT = "output"
    """Output tokens (completion, response, etc.)."""
