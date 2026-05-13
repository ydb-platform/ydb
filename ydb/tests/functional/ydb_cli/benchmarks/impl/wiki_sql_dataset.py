import json
import logging
import re
import pexpect
import pyarrow.parquet as pq
import time
import random
import urllib.request
import ydb

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .benchmark_abstract import BenchmarkAbstract, BenchmarkSample, DatabaseAccessor
from .common import sanitize_identifier, sanitize_column_names, build_upsert_query, build_create_query

# WikiSQL aggregation/condition operator tables - see https://github.com/salesforce/WikiSQL
_WIKISQL_AGG_OPS = ("", "MAX", "MIN", "COUNT", "SUM", "AVG")
_WIKISQL_COND_OPS = ("=", ">", "<")


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None


def _row_matches(row: List[Any], conds: Dict[str, Any]) -> bool:
    column_indices = list(conds.get("column_index") or [])
    operator_indices = list(conds.get("operator_index") or [])
    conditions = list(conds.get("condition") or [])
    for col_idx, op_idx, cond_value in zip(column_indices, operator_indices, conditions):
        if col_idx is None or col_idx >= len(row):
            return False
        cell = row[col_idx]
        op = _WIKISQL_COND_OPS[op_idx] if 0 <= op_idx < len(_WIKISQL_COND_OPS) else "="
        if op == "=":
            if str(cell).strip().lower() != str(cond_value).strip().lower():
                return False
        else:
            cell_num = _to_float(cell)
            cond_num = _to_float(cond_value)
            if cell_num is None or cond_num is None:
                return False
            if op == ">" and not (cell_num > cond_num):
                return False
            if op == "<" and not (cell_num < cond_num):
                return False
    return True


def evaluate_wiki_sql(table: Dict[str, Any], sql: Dict[str, Any]) -> Optional[str]:
    """Run the structured WikiSQL query against the table and return the answer as a string."""
    rows = list(table.get("rows") or [])
    sel = sql.get("sel")
    agg = sql.get("agg") or 0
    conds = sql.get("conds") or {}
    if sel is None:
        return None

    matched_rows = [row for row in rows if _row_matches(row, conds)]
    values = [row[sel] for row in matched_rows if sel < len(row)]

    if agg == 0:
        if not values:
            return ""
        return str(values[0])
    if agg == 3:  # COUNT
        return str(len(values))

    nums = [n for n in (_to_float(v) for v in values) if n is not None]
    if not nums:
        return ""
    if agg == 1:
        return _format_number(max(nums))
    if agg == 2:
        return _format_number(min(nums))
    if agg == 4:
        return _format_number(sum(nums))
    if agg == 5:
        return _format_number(sum(nums) / len(nums))
    return None


def _format_number(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return f"{value:g}"


def _normalize(text: Optional[str]) -> str:
    _PUNCT_RE = re.compile(r"[\s\W_]+", re.UNICODE)
    if text is None:
        return ""
    return _PUNCT_RE.sub("", str(text)).lower()


def answers_match(expected: Optional[str], extracted: Optional[str]) -> bool:
    if expected is None or extracted is None:
        return False
    expected_num = _to_float(expected)
    extracted_num = _to_float(extracted)
    if expected_num is not None and extracted_num is not None:
        return abs(expected_num - extracted_num) < 1e-6
    norm_expected = _normalize(expected)
    norm_extracted = _normalize(extracted)
    if not norm_expected or not norm_extracted:
        return False
    return norm_expected == norm_extracted or norm_expected in norm_extracted


class WikiSqlSample(BenchmarkSample):
    _YDB_PRIMITIVE_TYPES = {
        "text": ydb.PrimitiveType.Utf8,
        "real": ydb.PrimitiveType.Double,
    }
    _AI_PROMPT_REGEX = r"AI\x1b\[22;39m>"
    _ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
    _AUDIT_TOOL_CALL_RE = re.compile(r"\[AUDIT\] tool_call (\{.+\})")
    _AUDIT_AGENT_RESPONSE_RE = re.compile(r"\[AUDIT\] agent_response (\{.+\})")

    RESULT_START_MARKER = "<BENCHMARK_RESULT>"
    RESULT_END_MARKER = "</BENCHMARK_RESULT>"
    RESULT_INSTRUCTION = (
        "When you have the final answer, output it on a single line wrapped between "
        f"`{RESULT_START_MARKER}` and `{RESULT_END_MARKER}` markers, then stop responding. "
        f"Use the markers exactly once. Example: {RESULT_START_MARKER}your answer{RESULT_END_MARKER}"
    )
    DRY_RUN_INSTRUCTION = (
        "All tools are disabled for this run. Provide your best instant answer based solely on the "
        "question text, without inspecting any tables, schemas, or running any queries."
    )
    INTERACTIVE_BANNER_TIMEOUT = 30

    @classmethod
    def _parse_audit_records(cls, raw_output: str, pattern: re.Pattern) -> List[Dict[str, Any]]:
        clean = cls._ANSI_ESCAPE_RE.sub("", raw_output)
        records: List[Dict[str, Any]] = []
        for line in clean.splitlines():
            match = pattern.search(line)
            if not match:
                continue
            try:
                records.append(json.loads(match.group(1)))
            except json.JSONDecodeError:
                continue
        return records

    @classmethod
    def _parse_tool_calls(cls, raw_output: str) -> List[Dict[str, Any]]:
        """Extract structured tool-call records emitted by the AI handler at Info level."""
        return cls._parse_audit_records(raw_output, cls._AUDIT_TOOL_CALL_RE)

    @classmethod
    def _parse_agent_responses(cls, raw_output: str) -> List[Dict[str, Any]]:
        """Extract structured agent-response records emitted by the AI handler at Info level."""
        return cls._parse_audit_records(raw_output, cls._AUDIT_AGENT_RESPONSE_RE)

    def __init__(self, model: str, target: dict, side_tables: List[dict], sample_timeout: int, dry_run: bool = False):
        self.logger = logging.getLogger("wiki_sql_sample")
        self.model = model
        self.target = target
        self.side_tables = side_tables
        self.sample_timeout = sample_timeout
        self.dry_run = dry_run

    @staticmethod
    def _build_table_path(table: dict) -> str:
        path = ""
        page_title = (table.get("page_title") or "").strip()
        if page_title:
            path += sanitize_identifier(page_title, fallback_prefix="p_") + "/"

        section_title = (table.get("section_title") or table.get("caption") or "").strip()
        if section_title:
            path += sanitize_identifier(section_title, fallback_prefix="s_") + "_"

        raw_id = (table.get("id") or "table").strip()
        path += sanitize_identifier(raw_id, fallback_prefix="i_")
        return path

    def _create_and_populate_table(self, database: DatabaseAccessor, table: dict) -> str:
        columns = sanitize_column_names(table.get("header", []), fallback_prefix="c_")
        types = list(map(WikiSqlSample._YDB_PRIMITIVE_TYPES.get, table.get("types", [])))
        if len(types) != len(columns):
            types = (types + [ydb.PrimitiveType.Utf8] * len(columns))[: len(columns)]

        table_id = self._build_table_path(table)
        database.execute_query(build_create_query(table_id, columns, types))
        database.execute_query(*build_upsert_query(table_id, columns, types, list(table.get("rows", []))))
        return table_id

    def run(self, database: DatabaseAccessor) -> Any:
        started_at = time.monotonic()
        if self.dry_run:
            target_table_id = ""
            side_table_ids: List[str] = []
        else:
            target_table_id = self._create_and_populate_table(database, self.target["table"])
            side_table_ids = [self._create_and_populate_table(database, t) for t in self.side_tables]

        child = database.run_interactive(self.model, timeout=self.sample_timeout)
        captured: List[str] = []
        timed_out = False
        try:
            child.expect(self._AI_PROMPT_REGEX, timeout=self.INTERACTIVE_BANNER_TIMEOUT)
            question = self.target.get("question", "")
            instructions = self.RESULT_INSTRUCTION
            if self.dry_run:
                instructions = f"{self.DRY_RUN_INSTRUCTION}\n\n{instructions}"
            child.sendline(f"{question}\n\n{instructions}")
            child.send("\r")

            evaluation_started_at = time.monotonic()
            deadline = started_at + self.sample_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                idx = child.expect([self._AI_PROMPT_REGEX, pexpect.TIMEOUT], timeout=remaining)
                captured.append(child.before or "")
                if idx == 0:
                    break
        finally:
            try:
                child.sendline("exit")
            except pexpect.exceptions.ExceptionPexpect:
                pass
            try:
                child.close(force=True)
            except pexpect.exceptions.ExceptionPexpect:
                pass

        response = "".join(captured)
        match = re.search(
            rf".*Agent response.*?{re.escape(self.RESULT_START_MARKER)}\s*(.*?)\s*{re.escape(self.RESULT_END_MARKER)}",
            response,
            re.DOTALL,
        )
        extracted = match.group(1).strip() if match else None
        finished_at = time.monotonic()

        try:
            expected_answer = evaluate_wiki_sql(self.target.get("table", {}), self.target.get("sql", {}))
        except Exception as exc:
            self.logger.warning(f"Failed to evaluate expected answer: {exc}")
            expected_answer = None

        sql = self.target.get("sql", {})
        agg_idx = sql.get("agg") or 0
        aggregation = _WIKISQL_AGG_OPS[agg_idx] if 0 < agg_idx < len(_WIKISQL_AGG_OPS) else ""

        return {
            "model": self.model,
            "question": question,
            "expected_sql": sql.get("human_readable", ""),
            "expected_answer": expected_answer,
            "aggregation": aggregation,
            "target_table_id": target_table_id,
            "side_table_ids": side_table_ids,
            "tool_calls": self._parse_tool_calls(response),
            "agent_responses": self._parse_agent_responses(response),
            "extracted_answer": extracted,
            "matched": answers_match(expected_answer, extracted),
            "timed_out": timed_out,
            "elapsed_seconds": finished_at - started_at,
            "evaluation_elapsed_seconds": finished_at - evaluation_started_at,
        }


class WikiSqlBenchmark(BenchmarkAbstract):
    WIKISQL_BASE_URL = "https://huggingface.co/datasets/Salesforce/wikisql/resolve/refs%2Fconvert%2Fparquet/default"
    DEFAULT_TABLES_COUNT = 10
    DEFAULT_SAMPLE_RATE = 0.1
    DEFAULT_SPLITS: Tuple[str, ...] = ("train", "validation", "test")
    DEFAULT_DATA_DIR = "wiki_sql_data"
    RANDOM_SEED = 42
    DEFAULT_SAMPLE_TIMEOUT = 60

    def __init__(self, config: dict):
        self.logger = logging.getLogger("wiki_sql_benchmark")

        models = config.get("models")
        if not models:
            raise ValueError("WikiSQL benchmark requires `models` to list at least one model name")
        if not isinstance(models, list) or not all(isinstance(m, str) for m in models):
            raise ValueError(f"WikiSQL benchmark `models` must be a list of model names, got: {models!r}")

        self.tables_count = int(config.get("tables_count", self.DEFAULT_TABLES_COUNT))
        if self.tables_count < 1:
            raise ValueError(f"`tables_count` must be >= 1, got {self.tables_count}")

        self.sample_rate = float(config.get("sample_rate", self.DEFAULT_SAMPLE_RATE))
        if not 0 < self.sample_rate <= 1:
            raise ValueError(f"`sample_rate` must be in (0, 1], got {self.sample_rate}")

        self.splits: List[str] = list(config.get("splits", self.DEFAULT_SPLITS))
        if not self.splits:
            raise ValueError("`splits` must list at least one split")

        self.sample_timeout = int(config.get("sample_timeout", self.DEFAULT_SAMPLE_TIMEOUT))
        if self.sample_timeout < 1:
            raise ValueError(f"`sample_timeout` must be >= 1, got {self.sample_timeout}")

        self.data_dir = Path(config.get("data_dir", self.DEFAULT_DATA_DIR))
        self.models = list(models)
        self.dry_run = bool(config.get("dry_run", False))
        self.include_samples = bool(config.get("include_samples", False))
        self._rng = random.Random(self.RANDOM_SEED)
        self._samples: List[BenchmarkSample] = self._build_samples()

    def _load_split(self, split: str) -> List[Dict[str, Any]]:
        parquet_path = self.data_dir / f"{split}.parquet"
        if not parquet_path.exists():
            self._download_split(split, parquet_path)

        table = pq.read_table(parquet_path)
        columns = table.to_pydict()
        keys = list(columns.keys())
        length = len(columns[keys[0]]) if keys else 0
        return [{k: columns[k][i] for k in keys} for i in range(length)]

    def _download_split(self, split: str, target: Path) -> None:
        url = f"{self.WIKISQL_BASE_URL}/{split}/0000.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        self.logger.info(f"Downloading WikiSQL split `{split}` from {url} to {target}")
        try:
            urllib.request.urlretrieve(url, tmp)
            tmp.replace(target)
        except BaseException:
            if tmp.exists():
                tmp.unlink()
            raise

    def _build_samples(self) -> List[BenchmarkSample]:
        samples: List[BenchmarkSample] = []
        for split in self.splits:
            rows = self._load_split(split)
            if not rows:
                self.logger.warning(f"WikiSQL split `{split}` is empty in {self.data_dir}")
                continue

            target_count = max(1, int(len(rows) * self.sample_rate))
            target_indices = self._rng.sample(range(len(rows)), min(target_count, len(rows)))

            for target_idx in target_indices:
                target = rows[target_idx]
                side_count = min(self.tables_count - 1, len(rows) - 1)
                side_tables: List[dict] = []
                if side_count > 0:
                    chosen = self._rng.sample(range(len(rows)), side_count + 1)
                    chosen = [i for i in chosen if i != target_idx][:side_count]
                    side_tables = [rows[i]["table"] for i in chosen]
                for model in self.models:
                    samples.append(WikiSqlSample(model, target, side_tables, self.sample_timeout, self.dry_run))

        return samples

    def __len__(self) -> int:
        return len(self._samples)

    def __iter__(self) -> Iterator[BenchmarkSample]:
        return iter(self._samples)

    @staticmethod
    def _aggregate_tool_call_stats(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not samples:
            return {
                "total": 0,
                "avg_per_run": 0.0,
                "failed": {"avg_per_run": 0.0, "max_per_run": 0, "list": []},
                "runs_without_tool": {},
            }

        all_tool_names: set = set()
        per_run_calls: List[List[Dict[str, Any]]] = []
        for sample in samples:
            calls = sample.get("tool_calls") or []
            per_run_calls.append(calls)
            for call in calls:
                name = call.get("name")
                if name:
                    all_tool_names.add(name)

        total_calls = sum(len(c) for c in per_run_calls)
        failed_per_run = [sum(1 for call in c if not call.get("success", True)) for c in per_run_calls]
        failed_list: List[Dict[str, Any]] = []
        for sample, calls in zip(samples, per_run_calls):
            for call in calls:
                if not call.get("success", True):
                    failed_list.append(
                        {
                            "run_seq_no": sample.get("run_seq_no"),
                            "tool": call.get("name"),
                            "args": call.get("args"),
                            "result": call.get("result"),
                        }
                    )

        runs_without_tool: Dict[str, int] = {}
        for tool in all_tool_names:
            runs_without_tool[tool] = sum(1 for calls in per_run_calls if not any(c.get("name") == tool for c in calls))

        return {
            "total": total_calls,
            "avg_per_run": total_calls / len(samples),
            "failed": {
                "avg_per_run": sum(failed_per_run) / len(samples),
                "max_per_run": max(failed_per_run) if failed_per_run else 0,
                "list": failed_list,
            },
            "runs_without_tool": runs_without_tool,
        }

    @staticmethod
    def _aggregate_match_stats(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        matched = sum(1 for s in samples if s.get("matched"))
        unmatched: List[Dict[str, Any]] = []
        for sample in samples:
            if sample.get("matched"):
                continue
            unmatched.append(
                {
                    "run_seq_no": sample.get("run_seq_no"),
                    "model": sample.get("model"),
                    "question": sample.get("question"),
                    "expected_sql": sample.get("expected_sql"),
                    "expected_answer": sample.get("expected_answer"),
                    "extracted_answer": sample.get("extracted_answer"),
                    "target_table_id": sample.get("target_table_id"),
                    "timed_out": bool(sample.get("timed_out")),
                    "tool_calls": sample.get("tool_calls") or [],
                    "agent_responses": sample.get("agent_responses") or [],
                }
            )
        return {"matched": matched, "unmatched": unmatched}

    @staticmethod
    def _aggregate_aggregation_stats(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        agg_samples = [s for s in samples if s.get("aggregation")]
        per_op: Dict[str, Dict[str, int]] = {}
        for sample in agg_samples:
            op = sample.get("aggregation") or "UNKNOWN"
            stats = per_op.setdefault(op, {"total": 0, "with_extracted_answer": 0, "matched": 0})
            stats["total"] += 1
            if sample.get("extracted_answer"):
                stats["with_extracted_answer"] += 1
            if sample.get("matched"):
                stats["matched"] += 1
        return {
            "total": len(agg_samples),
            "with_extracted_answer": sum(1 for s in agg_samples if s.get("extracted_answer")),
            "matched": sum(1 for s in agg_samples if s.get("matched")),
            "per_operator": per_op,
        }

    def collect_statistics(self, samples: List[Any], statistics_path: str) -> None:
        path = Path(statistics_path) / "wiki-sql.json"
        path.parent.mkdir(parents=True, exist_ok=True)

        normalized: List[Dict[str, Any]] = []
        timed_out = 0
        with_answer = 0
        per_model_samples: Dict[str, List[Dict[str, Any]]] = {}
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            normalized.append(sample)
            model = sample.get("model", "unknown")
            per_model_samples.setdefault(model, []).append(sample)
            if sample.get("timed_out"):
                timed_out += 1
            if sample.get("extracted_answer"):
                with_answer += 1

        per_model: Dict[str, Dict[str, Any]] = {}
        for model, model_samples in per_model_samples.items():
            match_stats = self._aggregate_match_stats(model_samples)
            per_model[model] = {
                "total": len(model_samples),
                "timed_out": sum(1 for s in model_samples if s.get("timed_out")),
                "with_extracted_answer": sum(1 for s in model_samples if s.get("extracted_answer")),
                "matched": match_stats["matched"],
                "unmatched": match_stats["unmatched"],
                "aggregation": self._aggregate_aggregation_stats(model_samples),
                "tool_calls": self._aggregate_tool_call_stats(model_samples),
            }

        match_stats = self._aggregate_match_stats(normalized)
        summary = {
            "total": len(normalized),
            "timed_out": timed_out,
            "with_extracted_answer": with_answer,
            "matched": match_stats["matched"],
            "unmatched": match_stats["unmatched"],
            "aggregation": self._aggregate_aggregation_stats(normalized),
            "tool_calls": self._aggregate_tool_call_stats(normalized),
            "per_model": per_model,
        }
        if self.include_samples:
            summary["samples"] = normalized
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        self.logger.info(f"Wrote wiki-sql statistics to {path}")
