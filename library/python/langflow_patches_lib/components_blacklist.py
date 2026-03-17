from . import utils

# Hidden bundles - entire top-level directories that should be hidden
# They are not forbidden, just don't work *now*
HIDDEN_BUNDLES = {
    "Notion",
    "agentql",
    "aiml",
    "amazon",
    "anthropic",
    "apify",
    "arxiv",
    "assemblyai",
    "azure",
    "baidu",
    "bing",
    "chains",
    "cleanlab",
    "cloudflare",
    "cohere",
    "composio",
    "confluence",
    "crewai",
    "datastax",
    "deepseek",
    "docling",
    "documentloaders",
    "duckduckgo",
    "embeddings",
    "exa",
    "firecrawl",
    "git",
    "glean",
    "google",
    "groq",
    "homeassistant",
    "huggingface",
    "ibm",
    "icosacomputing",
    "jigsawstack",
    "langchain_utilities",
    "langwatch",
    "link_extractors",
    "lmstudio",
    "maritalk",
    "mem0",
    "mistral",
    "needle",
    "notdiamond",
    "novita",
    "nvidia",
    "olivya",
    "ollama",
    "openai",
    "openrouter",
    "output_parsers",
    "perplexity",
    "prototypes",
    "redis",
    "sambanova",
    "scrapegraph",
    "searchapi",
    "serpapi",
    "tavily",
    "textsplitters",
    "tools",
    "twelvelabs",
    "unstructured",
    "vectara",
    "vectorstores",
    "vertexai",
    "wikipedia",
    "wolframalpha",
    "xai",
    "yahoosearch",
    "youtube",
    "zep",
}

HIDDEN_COMPONENTS = {"langflow.components.agents.mcp_component", "langflow.components.data.api_request"}

# Banned components - specific full paths to dangerous components
# These ARE FORBIDDEN
BANNED_COMPONENTS = {
    "langflow.components.tools.python_repl",
    "langflow.components.tools.python_code_structured_tool",
    "langflow.components.processing.python_repl_core",
    "langflow.components.prototypes.python_function",
    "langflow.components.custom_component.custom_component",
}


def is_hidden(bundle_name: str, component_name: str) -> bool:
    """
    Check if a bundle (top-level directory) should be hidden.

    Args:
        bundle_name: The bundle name (e.g., 'openai', 'anthropic')

    Returns:
        True if the bundle should be hidden, False otherwise
    """
    return bundle_name in HIDDEN_BUNDLES or component_name in HIDDEN_COMPONENTS


def is_banned(component_path: str) -> bool:
    """
    Check if a specific component path should be banned.

    Args:
        component_path: The full component path (e.g., 'processing.python_repl')


    Returns:
        True if the component should be banned, False otherwise
    """
    if utils.is_custom_code_eval_enabled():
        return False
    return component_path in BANNED_COMPONENTS
