import logging
from collections.abc import Sequence
from pathlib import Path

from clickhouse_connect.driver.exceptions import ProgrammingError

logger = logging.getLogger(__name__)


class ExternalFile:
    def __init__(
        self,
        file_path: str | None = None,
        file_name: str | None = None,
        data: bytes | None = None,
        fmt: str | None = None,
        types: str | Sequence[str] | None = None,
        structure: str | Sequence[str] | None = None,
        mime_type: str | None = None,
    ):
        if file_path:
            if data:
                raise ProgrammingError("Only data or file_path should be specified for external data, not both")
            try:
                with open(file_path, "rb") as file:
                    self.data = file.read()
            except OSError as ex:
                raise ProgrammingError(f"Failed to open file {file_path} for external data") from ex
            path_name = Path(file_path).name
            path_base = path_name.rsplit(".", maxsplit=1)[0]
            if not file_name:
                self.name = path_base
                self.file_name = path_name
            else:
                self.name = file_name.rsplit(".", maxsplit=1)[0]
                self.file_name = file_name
                if file_name != path_name and path_base != self.name:
                    logger.warning("External data name %s and file_path %s use different names", file_name, path_name)
        elif data is not None:
            if not file_name:
                raise ProgrammingError("Name is required for query external data")
            self.data = data
            self.name = file_name.rsplit(".", maxsplit=1)[0]
            self.file_name = file_name
        else:
            raise ProgrammingError("Either data or file_path must be specified for external data")
        self.structure = None
        self.types = None
        if types:
            if structure:
                raise ProgrammingError("Only types or structure should be specified for external data, not both")
            if isinstance(types, str):
                self.types = types
            else:
                self.types = ",".join(types)
        elif structure:
            if isinstance(structure, str):
                self.structure = structure
            else:
                self.structure = ",".join(structure)
        self.fmt = fmt
        self.mime_type = mime_type or "application/octet-stream"

    @property
    def form_data(self) -> tuple:
        return self.file_name, self.data, self.mime_type

    @property
    def query_params(self) -> dict[str, str]:
        params = {}
        for name, value in (("format", self.fmt), ("structure", self.structure), ("types", self.types)):
            if value:
                params[f"{self.name}_{name}"] = value
        return params


class ExternalData:
    def __init__(
        self,
        file_path: str | None = None,
        file_name: str | None = None,
        data: bytes | None = None,
        fmt: str | None = None,
        types: str | Sequence[str] | None = None,
        structure: str | Sequence[str] | None = None,
        mime_type: str | None = None,
    ):
        self.files: list[ExternalFile] = []
        if file_path or data is not None:
            first_file = ExternalFile(
                file_path=file_path, file_name=file_name, data=data, fmt=fmt, types=types, structure=structure, mime_type=mime_type
            )
            self.files.append(first_file)

    def add_file(
        self,
        file_path: str | None = None,
        file_name: str | None = None,
        data: bytes | None = None,
        fmt: str | None = None,
        types: str | Sequence[str] | None = None,
        structure: str | Sequence[str] | None = None,
        mime_type: str | None = None,
    ):
        self.files.append(
            ExternalFile(
                file_path=file_path, file_name=file_name, data=data, fmt=fmt, types=types, structure=structure, mime_type=mime_type
            )
        )

    @property
    def form_data(self) -> dict[str, tuple]:
        if not self.files:
            raise ProgrammingError("No external files set for external data")
        return {file.name: file.form_data for file in self.files}

    @property
    def query_params(self) -> dict[str, str]:
        if not self.files:
            raise ProgrammingError("No external files set for external data")
        params = {}
        for file in self.files:
            params.update(file.query_params)
        return params
