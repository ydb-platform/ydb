#!/usr/bin/env python3
import json
import sys
from pathlib import Path


ROOT = Path(__file__).parents[1]
EVALS = ROOT / "evals" / "evals.json"
REQUIRED = {"id", "prompt", "expected_output", "files", "expectations", "covers"}


def fail(message: str) -> None:
    raise ValueError(f"{EVALS}: {message}")


def main() -> int:
    try:
        payload = json.loads(EVALS.read_text(encoding="utf-8"))
        if payload.get("skill_name") != "ydb-release-publication":
            fail("skill_name must be ydb-release-publication")
        cases = payload.get("evals")
        if not isinstance(cases, list) or not cases:
            fail("evals must be a non-empty list")
        ids = []
        for case in cases:
            if not isinstance(case, dict) or REQUIRED - case.keys():
                fail("every case must contain the required fields")
            if not isinstance(case["id"], int) or case["id"] <= 0:
                fail("case id must be a positive integer")
            ids.append(case["id"])
            if not all(isinstance(case[field], str) and case[field].strip() for field in ("prompt", "expected_output", "covers")):
                fail(f"case {case['id']}: text fields must be non-empty")
            if not isinstance(case["files"], list) or not case["files"]:
                fail(f"case {case['id']}: files must be a non-empty list")
            if not isinstance(case["expectations"], list) or not case["expectations"]:
                fail(f"case {case['id']}: expectations must be a non-empty list")
            for fixture in case["files"]:
                if not isinstance(fixture, str) or not (EVALS.parent / fixture).is_file():
                    fail(f"case {case['id']}: missing fixture {fixture}")
        if len(ids) != len(set(ids)):
            fail("case ids must be unique")
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1

    print(f"Validated {len(cases)} release-publication evals")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
