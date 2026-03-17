from __future__ import annotations

import subprocess  # noqa: S404
from enum import Enum
from functools import cached_property
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any
from warnings import warn

import black
import isort

from datamodel_code_generator.util import load_toml

try:
    import black.mode
except ImportError:  # pragma: no cover
    black.mode = None


class DatetimeClassType(Enum):
    Datetime = "datetime"
    Awaredatetime = "AwareDatetime"
    Naivedatetime = "NaiveDatetime"


class PythonVersion(Enum):
    PY_39 = "3.9"
    PY_310 = "3.10"
    PY_311 = "3.11"
    PY_312 = "3.12"
    PY_313 = "3.13"
    PY_314 = "3.14"

    @cached_property
    def _is_py_310_or_later(self) -> bool:  # pragma: no cover
        return self.value != self.PY_39.value

    @cached_property
    def _is_py_311_or_later(self) -> bool:  # pragma: no cover
        return self.value not in {self.PY_39.value, self.PY_310.value}

    @property
    def has_union_operator(self) -> bool:  # pragma: no cover
        return self._is_py_310_or_later

    @property
    def has_typed_dict_non_required(self) -> bool:
        return self._is_py_311_or_later

    @property
    def has_kw_only_dataclass(self) -> bool:
        return self._is_py_310_or_later


PythonVersionMin = PythonVersion.PY_39

if TYPE_CHECKING:
    from collections.abc import Sequence

    class _TargetVersion(Enum): ...

    BLACK_PYTHON_VERSION: dict[PythonVersion, _TargetVersion]
else:
    BLACK_PYTHON_VERSION: dict[PythonVersion, black.TargetVersion] = {
        v: getattr(black.TargetVersion, f"PY{v.name.split('_')[-1]}")
        for v in PythonVersion
        if hasattr(black.TargetVersion, f"PY{v.name.split('_')[-1]}")
    }


def is_supported_in_black(python_version: PythonVersion) -> bool:  # pragma: no cover
    return python_version in BLACK_PYTHON_VERSION


def black_find_project_root(sources: Sequence[Path]) -> Path:
    if TYPE_CHECKING:
        from collections.abc import Iterable  # noqa: PLC0415

        def _find_project_root(
            srcs: Sequence[str] | Iterable[str],
        ) -> tuple[Path, str] | Path: ...

    else:
        from black import find_project_root as _find_project_root  # noqa: PLC0415
    project_root = _find_project_root(tuple(str(s) for s in sources))
    if isinstance(project_root, tuple):
        return project_root[0]
    # pragma: no cover
    return project_root


class Formatter(Enum):
    BLACK = "black"
    ISORT = "isort"
    RUFF_CHECK = "ruff-check"
    RUFF_FORMAT = "ruff-format"


DEFAULT_FORMATTERS = [Formatter.BLACK, Formatter.ISORT]


class CodeFormatter:
    def __init__(  # noqa: PLR0912, PLR0913, PLR0917
        self,
        python_version: PythonVersion,
        settings_path: Path | None = None,
        wrap_string_literal: bool | None = None,  # noqa: FBT001
        skip_string_normalization: bool = True,  # noqa: FBT001, FBT002
        known_third_party: list[str] | None = None,
        custom_formatters: list[str] | None = None,
        custom_formatters_kwargs: dict[str, Any] | None = None,
        encoding: str = "utf-8",
        formatters: list[Formatter] = DEFAULT_FORMATTERS,
    ) -> None:
        if not settings_path:
            settings_path = Path.cwd()

        root = black_find_project_root((settings_path,))
        path = root / "pyproject.toml"
        if path.is_file():
            pyproject_toml = load_toml(path)
            config = pyproject_toml.get("tool", {}).get("black", {})
        else:
            config = {}

        black_kwargs: dict[str, Any] = {}
        if wrap_string_literal is not None:
            experimental_string_processing = wrap_string_literal
        elif black.__version__ < "24.1.0":
            experimental_string_processing = config.get("experimental-string-processing")
        else:
            experimental_string_processing = config.get("preview", False) and (  # pragma: no cover
                config.get("unstable", False) or "string_processing" in config.get("enable-unstable-feature", [])
            )

        if experimental_string_processing is not None:  # pragma: no cover
            if black.__version__.startswith("19."):
                warn(
                    f"black doesn't support `experimental-string-processing` option"
                    f" for wrapping string literal in {black.__version__}",
                    stacklevel=2,
                )
            elif black.__version__ < "24.1.0":
                black_kwargs["experimental_string_processing"] = experimental_string_processing
            elif experimental_string_processing:
                black_kwargs["preview"] = True
                black_kwargs["unstable"] = config.get("unstable", False)
                black_kwargs["enabled_features"] = {black.mode.Preview.string_processing}

        if TYPE_CHECKING:
            self.black_mode: black.FileMode
        else:
            self.black_mode = black.FileMode(
                target_versions={BLACK_PYTHON_VERSION[python_version]},
                line_length=config.get("line-length", black.DEFAULT_LINE_LENGTH),
                string_normalization=not skip_string_normalization or not config.get("skip-string-normalization", True),
                **black_kwargs,
            )

        self.settings_path: str = str(settings_path)

        self.isort_config_kwargs: dict[str, Any] = {}
        if known_third_party:
            self.isort_config_kwargs["known_third_party"] = known_third_party

        if isort.__version__.startswith("4."):
            self.isort_config = None
        else:
            self.isort_config = isort.Config(settings_path=self.settings_path, **self.isort_config_kwargs)

        self.custom_formatters_kwargs = custom_formatters_kwargs or {}
        self.custom_formatters = self._check_custom_formatters(custom_formatters)
        self.encoding = encoding
        self.formatters = formatters

    def _load_custom_formatter(self, custom_formatter_import: str) -> CustomCodeFormatter:
        import_ = import_module(custom_formatter_import)

        if not hasattr(import_, "CodeFormatter"):
            msg = f"Custom formatter module `{import_.__name__}` must contains object with name CodeFormatter"
            raise NameError(msg)

        formatter_class = import_.__getattribute__("CodeFormatter")  # noqa: PLC2801

        if not issubclass(formatter_class, CustomCodeFormatter):
            msg = f"The custom module {custom_formatter_import} must inherit from `datamodel-code-generator`"
            raise TypeError(msg)

        return formatter_class(formatter_kwargs=self.custom_formatters_kwargs)

    def _check_custom_formatters(self, custom_formatters: list[str] | None) -> list[CustomCodeFormatter]:
        if custom_formatters is None:
            return []

        return [self._load_custom_formatter(custom_formatter_import) for custom_formatter_import in custom_formatters]

    def format_code(
        self,
        code: str,
    ) -> str:
        if Formatter.ISORT in self.formatters:
            code = self.apply_isort(code)
        if Formatter.BLACK in self.formatters:
            code = self.apply_black(code)

        if Formatter.RUFF_CHECK in self.formatters:
            code = self.apply_ruff_lint(code)

        if Formatter.RUFF_FORMAT in self.formatters:
            code = self.apply_ruff_formatter(code)

        for formatter in self.custom_formatters:
            code = formatter.apply(code)

        return code

    def apply_black(self, code: str) -> str:
        return black.format_str(
            code,
            mode=self.black_mode,
        )

    def apply_ruff_lint(self, code: str) -> str:
        result = subprocess.run(
            ("ruff", "check", "--fix", "-"),
            input=code.encode(self.encoding),
            capture_output=True,
            check=False,
        )
        return result.stdout.decode(self.encoding)

    def apply_ruff_formatter(self, code: str) -> str:
        result = subprocess.run(
            ("ruff", "format", "-"),
            input=code.encode(self.encoding),
            capture_output=True,
            check=False,
        )
        return result.stdout.decode(self.encoding)

    if TYPE_CHECKING:

        def apply_isort(self, code: str) -> str: ...

    elif isort.__version__.startswith("4."):

        def apply_isort(self, code: str) -> str:
            return isort.SortImports(
                file_contents=code,
                settings_path=self.settings_path,
                **self.isort_config_kwargs,
            ).output

    else:

        def apply_isort(self, code: str) -> str:
            return isort.code(code, config=self.isort_config)


class CustomCodeFormatter:
    def __init__(self, formatter_kwargs: dict[str, Any]) -> None:
        self.formatter_kwargs = formatter_kwargs

    def apply(self, code: str) -> str:
        raise NotImplementedError
