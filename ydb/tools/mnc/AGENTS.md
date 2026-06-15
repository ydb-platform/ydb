# MNC Instructions

## Python

- Projects under `ydb/tools/mnc` are Python projects built through `ya`.
- All available third-party Python packages should come from `contrib/python`.
- Do not add dependencies that are not available through `contrib/python` unless
  the dependency import path and `ya.make` integration are explicitly handled.

## Error Handling

- Do not call `sys.exit()` from library code or command implementation helpers.
- Raise exceptions and let `ydb/tools/mnc/cli/main.py` decide how to print the error and exit.
- Use `CliError` from `ydb.tools.mnc.lib.exceptions` for user-facing CLI failures.
- Use `ConfigError` for configuration loading or validation errors that should include a config path.
- Prefer preserving actionable error details in the exception message instead of printing before raising.

## Output and Logging

- All live console output should go through `output.get_console()` from `ydb.tools.mnc.lib.output`.
- Use `output.get_stderr_console()` for error messages that need to go to stderr.
- The `print()` function is forbidden in library and command code except in `main.py`.
- All logging should use the standard `logging` module with `logging.getLogger(__name__)`.
- Do not write directly to stdout/stderr from library code; return a `TaskResult` or raise `CliError` instead.
- Verbosity modes are controlled by `--verbose/-V` and `--quiet/-q` flags.
- Log levels by verbosity mode:
  - QUIET: WARNING
  - NORMAL: WARNING
  - VERBOSE: DEBUG
- Any `logger.error` for user-facing errors should be accompanied by returning a `TaskResult(ERROR, message=...)` to the caller.
- Long-running external command output should be persisted under `deploy_ctx.work_directory/logs` and user-facing errors should include a `Full log:` path.

## Command Contract

- All command `async def do(...)` functions must return `TaskResult` (not `bool`).
- Use `TaskResult(level=OK, ...)` for success and `TaskResult(level=ERROR, ...)` for failures.
- Commands should not return `None`, `0`, `[]`, `True`, `False`, or other raw values.
- The main loop checks `if not result:` to detect failures, which works with `TaskResult.__bool__()`.
- For backward compatibility during transition, `bool` values are still accepted but deprecated.
- Commands must include meaningful `step_title` and `message` in `TaskResult` for better error reporting.
- `act_*` functions should return `TaskResult`. If a transitional caller still needs a boolean, use `bool(result)` at the boundary.
- Command implementations should build user-visible work as `Step`, `SequentialStepGroup`, or `ParallelStepGroup` and render final results with `TaskResult.to_rich_panel(...)`.

## TUI and Progress

- `--tui` uses the Rich Live backend and must remain a rendering layer over the same `Step`/`TaskResult` execution model.
- `mnc` with no verb and `mnc install` should route to the launcher TUI for command/config/options collection.
- Runtime TUI should stay generic: tree, selected-node details, and live logs. Command-specific widgets should be added only when backed by reusable metadata.
- New progress renderers should implement the `ProgressBackend` interface and avoid changing command business logic.
- Live command output should be streamed through the active progress backend when it supports log ingestion, and still be saved to full log files.

## Testing

- Use `unittest` framework for all tests.
- For async tests, use `unittest.IsolatedAsyncioTestCase` as the base class.
- Test files should be placed in `cli/ut/` for CLI tests and `agent/ut/` for agent tests.
- Add test files to the `TEST_SRCS()` list in the appropriate `ya.make` file.
- Use `unittest.mock` for mocking dependencies and external services.
- Reset module-level state in `setUp()` methods when testing modules with global state (e.g., `output._state`).
- Test both success and failure paths for all functions.
- Test edge cases like None values, empty lists, and exception handling.
- For output-related tests, verify that the correct console methods are called and output format is correct.

## Code Style

- Keep comments minimal and focused on explaining concepts or complex logic.
- Comments should not be longer than the functions they describe.
- Small, simple functions should have no comments at all.
- Only add comments for non-obvious behavior, architectural decisions, or complex algorithms.
- Function and variable names should be self-documenting.
