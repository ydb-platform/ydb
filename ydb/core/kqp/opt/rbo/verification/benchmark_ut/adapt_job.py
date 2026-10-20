"""Adapt the pinned JOB SQL grammar without changing predicates or projections.

Usage: python3 adapt_job.py /path/to/downloaded/job /path/to/corpora/job
The input directory must contain source.json with revision and SHA-256 hashes.
Only the schema, FROM table paths and implicit-join pragma change.
Unknown grammar fails closed.
"""

import hashlib
import json
from pathlib import Path
import re
import sys


def adapt_query(source: str) -> str:
    select, rest = re.split(r"(?m)^FROM\s+", source)
    tables, predicate = re.split(r"(?m)^WHERE\s+", rest)
    adapted = []
    for entry in tables.strip().split(","):
        match = re.fullmatch(r"\s*([a-z_][a-z_0-9]*) AS ([a-z_][a-z_0-9]*)\s*", entry)
        if not match:
            raise ValueError(f"Unsupported JOB table reference: {entry!r}")
        table, alias = match.groups()
        adapted.append(f"`/Root/{table}` AS {alias}")
    return 'PRAGMA AnsiImplicitCrossJoin;\n' + select + "FROM " + ",\n     ".join(adapted) + "\nWHERE " + predicate


def adapt_schema(source: str) -> str:
    statements = []
    for statement in source.split(";"):
        if not statement.strip():
            continue
        match = re.fullmatch(r"\s*CREATE TABLE ([a-z_]+) \((.*?)\)\s*", statement, re.S)
        if not match:
            raise ValueError("Unsupported JOB schema statement")
        table, body = match.groups()
        columns = []
        keys = []
        for field in body.split(","):
            match = re.fullmatch(
                r"\s*([a-z_][a-z_0-9]*) (integer|text|character varying\(\d+\))"
                r"( NOT NULL)?( PRIMARY KEY)?\s*", field)
            if not match:
                raise ValueError(f"Unsupported JOB column: {field!r}")
            name, kind, required, primary = match.groups()
            columns.append(f"    {name} {'Int32' if kind == 'integer' else 'Utf8'}{required or ''}")
            if primary:
                keys.append(name)
        if keys != ["id"]:
            raise ValueError(f"Unexpected JOB primary key: {keys!r}")
        columns.append("    PRIMARY KEY (id)")
        statements.append(f"CREATE TABLE `/Root/{table}` (\n" + ",\n".join(columns) + "\n);")
    return "\n\n".join(statements) + "\n"


def generate(source: Path, destination: Path) -> None:
    provenance = json.loads((source / "source.json").read_text())
    names = sorted((name for name in provenance["files"] if name != "schema.sql"),
                   key=lambda name: (int(re.match(r"\d+", name)[0]), name))
    if len(names) != 113:
        raise ValueError("Expected the complete 113-query JOB corpus")
    records = []
    outputs = {}
    for number, name in [(0, "schema.sql"), *enumerate(names, 1)]:
        content = (source / name).read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        if digest != provenance["files"][name]:
            raise ValueError(f"Source hash mismatch: {name}")
        result = (adapt_schema if number == 0 else adapt_query)(content.decode())
        filename = "schema.yql" if number == 0 else f"q{number}.yql"
        outputs[filename] = result
        records.append({"query_id": number, "source": name, "source_sha256": digest,
                        "adapted": filename, "adapted_sha256": hashlib.sha256(result.encode()).hexdigest()})
    manifest = {"repository": provenance["repository"], "revision": provenance["revision"],
                "paper": "https://www.vldb.org/pvldb/vol9/p204-leis.pdf",
                "adaptations": ["FROM tables use /Root paths; projections and predicates are preserved",
                                "AnsiImplicitCrossJoin enables SQL comma joins before WHERE filtering",
                                "integer maps to Int32; text/varchar map to Utf8 with YDB byte collation",
                                "varchar length restrictions are not modeled; the string domain is broader",
                                "primary keys and nullability are preserved; no IMDb data is downloaded"],
                "queries": records}
    outputs["source.json"] = json.dumps(manifest, indent=2) + "\n"
    destination.mkdir(parents=True, exist_ok=True)
    for filename, content in outputs.items():
        (destination / filename).write_text(content)


if __name__ == "__main__":
    generate(Path(sys.argv[1]), Path(sys.argv[2]))
