# Allowed bundles - top-level directories that are allowed
ALLOWED_BUNDLES = {
    "clickhouse",
    "custom_component",
    "data",
    "data_source",
    "files_and_knowledge",
    "flow_controls",
    "helpers",
    "input_output",
    "knowledge_bases",
    "langchain_utilities",
    "llm_operations",
    "logic",
    "models",
    "models_and_agents",
    "processing",
    "utilities",
    "vectorstores",
}

# Banned components within allowed bundles - specific components that should be excluded
# These are dangerous components that can execute arbitrary code
BANNED_COMPONENTS_IN_ALLOWED_BUNDLES = {
    "lfx.components.tools.python_repl",
    "lfx.components.tools.python_code_structured_tool",
    "lfx.components.utilities.python_repl_core",
    "lfx.components.prototypes.python_function",
    "lfx.components.custom_component.custom_component",
}


def is_allowed(bundle_name: str, component_path: str, dry_run: bool = False) -> bool:
    """
    Check if a component should be allowed based on whitelist.

    Args:
        bundle_name: The bundle name (e.g., 'data', 'tools')
        component_path: The full component path (e.g., 'lfx.components.tools.python_repl')
        dry_run: allow everything if True

    Returns:
        True if the component should be allowed, False otherwise
    """
    if dry_run:
        return True

    if bundle_name not in ALLOWED_BUNDLES:
        return False

    for banned in BANNED_COMPONENTS_IN_ALLOWED_BUNDLES:
        if component_path == banned or component_path.startswith(banned + "."):
            return False

    return True


def get_allowed_bundles():
    """Get the set of allowed bundle names."""
    return ALLOWED_BUNDLES.copy()


def get_banned_components():
    """Get the set of banned component paths."""
    return BANNED_COMPONENTS_IN_ALLOWED_BUNDLES.copy()
