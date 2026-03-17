"""@private"""

import re
from typing import Any, Dict, List, Literal, Optional, cast

# NOTE ON DEPENDENCIES:
# - since Jan 2024, there is https://pypi.org/project/langchain-openai/ which is a separate package and imports openai models.
#   Decided to not make this a dependency of langfuse as few people will have this. Need to match these models manually
# - langchain_community is loaded as a dependency of langchain, but extremely unreliable. Decided to not depend on it.


def _extract_model_name(
    serialized: Optional[Dict[str, Any]],
    **kwargs: Any,
) -> Optional[str]:
    """Extracts the model name from the serialized or kwargs object. This is used to get the model names for Langfuse."""
    # In this function we return on the first match, so the order of operations is important

    # First, extract known models where we know the model name aka id
    # Extract the model name from the provided path (aray) in the serialized or kwargs object
    models_by_id = [
        ("ChatGoogleGenerativeAI", ["kwargs", "model"], "serialized"),
        ("ChatMistralAI", ["kwargs", "model"], "serialized"),
        ("ChatVertexAi", ["kwargs", "model_name"], "serialized"),
        ("ChatVertexAI", ["kwargs", "model_name"], "serialized"),
        ("OpenAI", ["invocation_params", "model_name"], "kwargs"),
        ("ChatOpenAI", ["invocation_params", "model_name"], "kwargs"),
        ("AzureChatOpenAI", ["invocation_params", "model"], "kwargs"),
        ("AzureChatOpenAI", ["invocation_params", "model_name"], "kwargs"),
        ("AzureChatOpenAI", ["invocation_params", "azure_deployment"], "kwargs"),
        ("HuggingFacePipeline", ["invocation_params", "model_id"], "kwargs"),
        ("BedrockChat", ["kwargs", "model_id"], "serialized"),
        ("Bedrock", ["kwargs", "model_id"], "serialized"),
        ("BedrockLLM", ["kwargs", "model_id"], "serialized"),
        ("ChatBedrock", ["kwargs", "model_id"], "serialized"),
        ("LlamaCpp", ["invocation_params", "model_path"], "kwargs"),
        ("WatsonxLLM", ["invocation_params", "model_id"], "kwargs"),
    ]

    for model_name, keys, select_from in models_by_id:
        model = _extract_model_by_path_for_id(
            model_name,
            serialized,
            kwargs,
            keys,
            cast(Literal["serialized", "kwargs"], select_from),
        )
        if model:
            return model

    # Second, we match AzureOpenAI as we need to extract the model name, fdeployment version and deployment name
    if serialized:
        serialized_id = serialized.get("id")
        if (
            serialized_id
            and isinstance(serialized_id, list)
            and len(serialized_id) > 0
            and serialized_id[-1] == "AzureOpenAI"
        ):
            invocation_params = kwargs.get("invocation_params")
            if invocation_params and isinstance(invocation_params, dict):
                if invocation_params.get("model"):
                    return str(invocation_params.get("model"))

                if invocation_params.get("model_name"):
                    return str(invocation_params.get("model_name"))

            deployment_name = None
            deployment_version = None

            serialized_kwargs = serialized.get("kwargs")
            if serialized_kwargs and isinstance(serialized_kwargs, dict):
                if serialized_kwargs.get("openai_api_version"):
                    deployment_version = serialized_kwargs.get("deployment_version")

                if serialized_kwargs.get("deployment_name"):
                    deployment_name = serialized_kwargs.get("deployment_name")

            if not isinstance(deployment_name, str):
                return None

            if not isinstance(deployment_version, str):
                return deployment_name

            return (
                deployment_name + "-" + deployment_version
                if deployment_version not in deployment_name
                else deployment_name
            )

    # Third, for some models, we are unable to extract the model by a path in an object. Langfuse provides us with a string representation of the model pbjects
    # We use regex to extract the model from the repr string
    models_by_pattern = [
        ("Anthropic", "model", "anthropic"),
        ("ChatAnthropic", "model", None),
        ("ChatTongyi", "model_name", None),
        ("ChatCohere", "model", None),
        ("Cohere", "model", None),
        ("HuggingFaceHub", "model", None),
        ("ChatAnyscale", "model_name", None),
        ("TextGen", "model", "text-gen"),
        ("Ollama", "model", None),
        ("OllamaLLM", "model", None),
        ("ChatOllama", "model", None),
        ("ChatFireworks", "model", None),
        ("ChatPerplexity", "model", None),
        ("VLLM", "model", None),
        ("Xinference", "model_uid", None),
        ("ChatOCIGenAI", "model_id", None),
        ("DeepInfra", "model_id", None),
    ]

    for model_name, pattern, default in models_by_pattern:
        model = _extract_model_from_repr_by_pattern(
            model_name, serialized, pattern, default
        )
        if model:
            return model

    # Finally, we try to extract the most likely paths as a catch all
    random_paths = [
        ["kwargs", "model_name"],
        ["kwargs", "model"],
        ["invocation_params", "model_name"],
        ["invocation_params", "model"],
    ]
    for select in ["kwargs", "serialized"]:
        for path in random_paths:
            model = _extract_model_by_path(
                serialized, kwargs, path, cast(Literal["serialized", "kwargs"], select)
            )
            if model:
                return str(model)

    return None


def _extract_model_from_repr_by_pattern(
    id: str,
    serialized: Optional[Dict[str, Any]],
    pattern: str,
    default: Optional[str] = None,
) -> Optional[str]:
    if serialized is None:
        return None

    serialized_id = serialized.get("id")
    if (
        serialized_id
        and isinstance(serialized_id, list)
        and len(serialized_id) > 0
        and serialized_id[-1] == id
    ):
        repr_str = serialized.get("repr")
        if repr_str and isinstance(repr_str, str):
            extracted = _extract_model_with_regex(pattern, repr_str)
            return extracted if extracted else default if default else None

    return None


def _extract_model_with_regex(pattern: str, text: str) -> Optional[str]:
    match = re.search(rf"{pattern}='(.*?)'", text)
    if match:
        return match.group(1)
    return None


def _extract_model_by_path_for_id(
    id: str,
    serialized: Optional[Dict[str, Any]],
    kwargs: Dict[str, Any],
    keys: List[str],
    select_from: Literal["serialized", "kwargs"],
) -> Optional[str]:
    if serialized is None and select_from == "serialized":
        return None

    if serialized:
        serialized_id = serialized.get("id")
        if (
            serialized_id
            and isinstance(serialized_id, list)
            and len(serialized_id) > 0
            and serialized_id[-1] == id
        ):
            result = _extract_model_by_path(serialized, kwargs, keys, select_from)
            return str(result) if result is not None else None

    return None


def _extract_model_by_path(
    serialized: Optional[Dict[str, Any]],
    kwargs: dict,
    keys: List[str],
    select_from: Literal["serialized", "kwargs"],
) -> Optional[str]:
    if serialized is None and select_from == "serialized":
        return None

    current_obj = kwargs if select_from == "kwargs" else serialized

    for key in keys:
        if current_obj and isinstance(current_obj, dict):
            current_obj = current_obj.get(key)
        else:
            return None
        if not current_obj:
            return None

    return str(current_obj) if current_obj else None
