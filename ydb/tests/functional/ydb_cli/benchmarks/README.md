# AI Interactive CLI Benchmarks

This folder provides tool to run AI Interactive CLI against some well known datasets and measure:

- Results accuracy and success rate
- Proper tools usage

## Datasets

Here provided description and configuration for supported datasets.

### Wiki SQL

Each row in dataset contains target SQL query, table schema and data for this table.

Configuration example:

```json
{
    "models": {
        "yandex-gpt": {
            "model": "name",
            "endpoint": "localhost:12345",
            "api": "openai"
        }
    },
    "datasets": {
        "wiki-sql": {
            "models": ["yandex-gpt"],
            "tables_count": 10,
            "splits": ["train", "validation", "test"],
            "sample_rate": 0.1,
            "include_samples": false
        }
    }
}
```

- `models` - list of model names (from the top-level `models` map) to evaluate on this dataset
- `tables_count` - amount number of tables which will be created for one test run (1 with useful data and other tables to test schema navigation), default 10
- `splits` - on which splits benchmark should run
- `sample_rate` - how many random samples from split should be evaluated
- `include_samples` - include full statistics for each sample into final statistics

## Benchmark run

Example of starting benchmark (`config.json` - file with config):

```bash
export YDB_TOKEN=<token>
export MODEL_TOKEN=<token>
./ydb_cli_bench --config config.json --ydb-cli-binary ../ai_interactive/ydb --database /Root/shared_database --endpoint localhost:12345 --concurrency 3
```

Where `database` is path to shared database. Benchmark will be run over database from `database` / `endpoint` where will be created:

- `concurrency` count serverless databases `ydb_cli_bench_serverless_db_[0-concurrency]`

All schema object inside serverless database will be cleaned up before each sample. After benchmark databases wont be dropped, to drop them add flag `--cleanup`.

As database can be used tool `ydb/tests/tools/kqprun`:

```bash
./kqprun -M 33000 -G 12345 --shared shared_database
```
