# MNC Instructions

## Error Handling

- Do not call `sys.exit()` from library code or command implementation helpers.
- Raise exceptions and let `ydb/tools/mnc/cli/main.py` decide how to print the error and exit.
- Use `CliError` from `ydb.tools.mnc.lib.exceptions` for user-facing CLI failures.
- Use `ConfigError` for configuration loading or validation errors that should include a config path.
- Prefer preserving actionable error details in the exception message instead of printing before raising.
