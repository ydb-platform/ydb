# CI analytics

Клиент пишет длительности шагов в `analytics/ci_metrics`. Профили компиляции сюда не кладём.

| Файл | Роль |
| --- | --- |
| `ci_metrics.py` | CLI и GitHub-контекст (колонки workflow / job / PR / commit) |
| `core.py` | JSONL-буфер, `start` / `end` / `flush` / `send` |
| `runner_info.py` | cpu / ram / disk хоста (`--runner`, `--usage`) |
| `export_github_job_metrics.py` | очередь / job / step после факта |

```bash
python3 .github/scripts/utils/analytics/ci_metrics.py start my_step \
  --source ya_phase --label cache_mode=dist_cache
python3 .github/scripts/utils/analytics/ci_metrics.py end my_step --conclusion success
python3 .github/scripts/utils/analytics/ci_metrics.py enrich my_step --label report_url="$S3_URL"
python3 .github/scripts/utils/analytics/ci_metrics.py send
```

`flush` — только готовые строки. `send` — закрывает висящие спаны и выгружает всё. Labels: `cache_mode`, `ya_attempt`, `report_url`, `error`.

```bash
python3 -m unittest discover -s .github/scripts/utils/tests -p 'test_*.py'
```
