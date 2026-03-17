"""Prompts package for mcp-windbg."""

from pathlib import Path


def get_prompts_directory() -> Path:
    """Get the path to the prompts directory."""
    import importlib.resources
    return importlib.resources.files(__package__)


def load_prompt(name: str) -> str:
    """Load a prompt file by name.

    Args:
        name: The prompt name (without .prompt.md extension)

    Returns:
        The content of the prompt file

    Raises:
        FileNotFoundError: If the prompt file doesn't exist
    """
    prompts_dir = get_prompts_directory()
    prompt_path = prompts_dir / f"{name}.prompt.md"

    if not prompt_path.is_file():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    return prompt_path.read_text(encoding="utf-8")


def get_available_prompts() -> list[str]:
    """Get a list of available prompt names.

    Returns:
        List of prompt names (without .prompt.md extension)
    """
    prompts_dir = get_prompts_directory()
    prompt_files = [
        f for f in prompts_dir.iterdir() if f.name.endswith('.prompt.md')
    ]
    return [f.stem.replace(".prompt", "") for f in prompt_files]
