from typing import Any, Dict
from pathlib import Path

from datamodel_code_generator.format import CustomCodeFormatter


class CodeFormatter(CustomCodeFormatter):
    """Add a license to file from license file path."""

    def __init__(self, formatter_kwargs: Dict[str, Any]) -> None:
        super().__init__(formatter_kwargs)

        if "license_file" not in formatter_kwargs:
            raise ValueError()

        license_file_path = Path(formatter_kwargs["license_file"]).resolve()

        with license_file_path.open("r") as f:
            license_file = f.read()

        self.license_header = "\n".join([f"# {line}".strip() for line in license_file.split("\n")])

    def apply(self, code: str) -> str:
        return f"{self.license_header}\n{code}"
