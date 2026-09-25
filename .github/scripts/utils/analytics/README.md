# CI analytics

`collector/` — JSONL-буфер и flush в YDB. В другой проект копируется только эта папка.

`github_actions/` — колонки GitHub (workflow / job / PR / commit), `runner_info`, выгрузка job/step после факта.

Профили компиляции сюда не кладём. Таблица: `analytics/ci_metrics`.

```bash
python3 .github/scripts/utils/analytics/github_actions/ci_metrics.py start my_step \
  --source ya_phase --label cache_mode=dist_cache
python3 .github/scripts/utils/analytics/github_actions/ci_metrics.py end my_step --conclusion success
python3 .github/scripts/utils/analytics/github_actions/ci_metrics.py enrich my_step --label report_url="$S3_URL"
python3 .github/scripts/utils/analytics/github_actions/ci_metrics.py send
```

`flush` — только готовые строки. `send` — закрывает висящие спаны и выгружает всё. Labels: `cache_mode`, `ya_attempt`, `report_url`, `error`.

```bash
python3 -m unittest discover -s .github/scripts/utils/tests -p 'test_*.py'
```
