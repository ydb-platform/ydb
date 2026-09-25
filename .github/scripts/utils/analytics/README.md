# CI analytics

- [`collector/`](collector/README.md) — JSONL + YDB. Копируется в другой проект.
- [`github_actions/`](github_actions/README.md) — колонки GitHub, runner, export job/step.

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/collector -p 'test_*.py'
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/ci -p 'test_*.py'
```
