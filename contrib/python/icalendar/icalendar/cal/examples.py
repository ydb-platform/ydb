"""Access to examples of components."""

from __future__ import annotations

from pathlib import Path


def get_example(component_directory: str, example_name: str) -> bytes:
    """Return an example and raise an error if it is absent."""
    here = Path(__file__).parent.parent
    examples = here / "tests" / component_directory
    if not example_name.endswith(".ics"):
        example_name = example_name + ".ics"
    example_file = examples / example_name
    if not example_file.is_file():
        raise ValueError(
            f"Example {example_name} for {component_directory} not found. "
            f"You can use one of {', '.join(p.name for p in examples.iterdir())}"
        )
    return Path(example_file).read_bytes()


__all__ = ["get_example"]
