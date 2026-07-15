#!/usr/bin/env python3

import argparse
import json
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any

KNOWN_ATTRS = frozenset({"description", "min_langver", "max_langver"})


def parse_feature_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"invalid feature name: {name!r}")
    return name


def parse_attrs(name: str, attrs: dict[str, Any]) -> dict[str, Any]:
    unknown = attrs.keys() - KNOWN_ATTRS
    if unknown:
        raise ValueError(f"feature {name!r} has unknown attributes: {sorted(unknown)}")

    return attrs


def parse_langver(v: Any) -> str:
    if not isinstance(v, str):
        raise ValueError(f"language version must be a string: {v!r}")

    if v == 'max':
        return 'GetMaxLangVersion()'

    if v == 'unknown':
        return 'UnknownLangVersion'

    match = re.fullmatch(r"(\d{4})\.(\d{2})", v)
    if not match:
        raise ValueError(f"invalid language version: {v!r}")

    year, minor = int(match.group(1)), int(match.group(2))
    return f'MakeLangVersion({year}, {minor})'


def load(path: Path) -> dict[str, dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def emit_feature(name: str, attrs: dict[str, Any]) -> Generator[str]:
    name = parse_feature_name(name)
    attrs = parse_attrs(name, attrs)
    description = attrs.get("description", name)
    min_langver = parse_langver(attrs.get("min_langver", "unknown"))
    max_langver = parse_langver(attrs.get("max_langver", "unknown"))

    yield f'const TFeature {name} = {{'
    yield f'    .Name = "{name}",'
    yield f'    .Description = "{description}",'
    yield f'    .MinLangVer = {min_langver},'
    yield f'    .MaxLangVer = {max_langver},'
    yield f'}};'
    yield ""


def emit(features: dict[str, dict[str, Any]]) -> Generator[str]:
    yield "#pragma once"
    yield ""
    yield '#include "feature.h"'
    yield ""
    yield "namespace NYql::NFeature {"
    yield ""

    for name in sorted(features):
        yield from emit_feature(name, features[name])

    yield "} // namespace NYql::NFeature"
    yield ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    features = load(args.input)
    args.output.write_text("\n".join(emit(features)), encoding="utf-8")


if __name__ == "__main__":
    main()
