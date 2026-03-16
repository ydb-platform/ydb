""".. include:: ../README.md"""

from langfuse.batch_evaluation import (
    BatchEvaluationResult,
    BatchEvaluationResumeToken,
    CompositeEvaluatorFunction,
    EvaluatorInputs,
    EvaluatorStats,
    MapperFunction,
)
from langfuse.experiment import Evaluation

from ._client import client as _client_module
from ._client.attributes import LangfuseOtelSpanAttributes
from ._client.constants import ObservationTypeLiteral
from ._client.get_client import get_client
from ._client.observe import observe
from ._client.propagation import propagate_attributes
from ._client.span import (
    LangfuseAgent,
    LangfuseChain,
    LangfuseEmbedding,
    LangfuseEvaluator,
    LangfuseEvent,
    LangfuseGeneration,
    LangfuseGuardrail,
    LangfuseRetriever,
    LangfuseSpan,
    LangfuseTool,
)

Langfuse = _client_module.Langfuse

__all__ = [
    "Langfuse",
    "get_client",
    "observe",
    "propagate_attributes",
    "ObservationTypeLiteral",
    "LangfuseSpan",
    "LangfuseGeneration",
    "LangfuseEvent",
    "LangfuseOtelSpanAttributes",
    "LangfuseAgent",
    "LangfuseTool",
    "LangfuseChain",
    "LangfuseEmbedding",
    "LangfuseEvaluator",
    "LangfuseRetriever",
    "LangfuseGuardrail",
    "Evaluation",
    "EvaluatorInputs",
    "MapperFunction",
    "CompositeEvaluatorFunction",
    "EvaluatorStats",
    "BatchEvaluationResumeToken",
    "BatchEvaluationResult",
    "experiment",
    "api",
]
