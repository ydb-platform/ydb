# Minidumps (built-in Google Breakpad)

The `ydbd` nodes in {{ ydb-short-name }} can collect **minidumps** when the process crashes, using the built-in [Google Breakpad](https://chromium.googlesource.com/breakpad/breakpad/). A minidump is a compact snapshot of the process state (stack, registers, list of loaded modules) at the moment of a crash caused by an unhandled signal (`SIGSEGV`, `SIGABRT`, `SIGFPE`, and others).

Unlike a system [core dump](https://en.wikipedia.org/wiki/Core_dump), a minidump:

- takes up significantly less space;
- is collected by its own signal handler and does not depend on the host's `ulimit` and `core_pattern` settings;
- can be automatically passed to an external script for post-processing (uploading, symbolization, alerting).

{% note info %}

Minidump collection works on Linux only. On other platforms (for example, macOS) `ydbd` runs, but the built-in Breakpad is not part of the build and minidumps are not collected.

{% endnote %}

## Enabling minidump collection {#enable}

By default, minidump collection is disabled. To enable it, specify the directory where dumps will be written. This can be done in two ways: through environment variables or through `ydbd server` command-line options. Command-line options take precedence over environment variables.

If the directory is not set by either method, the built-in Breakpad is not activated and does not install signal handlers.

{% note info %}

The specified directory must exist before the process starts; {{ ydb-short-name }} does not create it automatically. If the directory is missing at crash time, the minidump will not be written.

{% endnote %}

### Environment variables {#env}

| Variable | Purpose |
|----------|---------|
| `BREAKPAD_MINIDUMPS_PATH` | directory for writing minidumps; setting this variable enables minidump collection at process startup |
| `BREAKPAD_MINIDUMPS_SCRIPT` | path to the minidump [post-processing](#script) script |

### Command-line options {#cli}

The options are added to the `ydbd server` command:

| Option | Purpose |
|--------|---------|
| `--breakpad-minidumps-path PATH` | directory for writing minidumps |
| `--breakpad-minidumps-script SCRIPT` | path to the minidump [post-processing](#script) script |

Startup example:

```bash
ydbd server \
    --breakpad-minidumps-path /var/log/ydb/minidumps \
    --breakpad-minidumps-script /usr/local/bin/process-minidump.sh \
    ...
```

## Post-processing script {#script}

If a post-processing script is set, it is run immediately after the minidump is written. The script is called with the following arguments:

| Argument | Value |
|----------|-------|
| `$1` | `true` or `false` — whether the minidump was written successfully |
| `$2` | path to the minidump file (empty string if writing failed) |

The `ydbd` process environment is not passed to the script, so any parameters it needs must be hardcoded in the script itself or derived from the arguments.

{% note warning %}

The script is launched from the crashed process's signal handler via `fork`/`exec`. The signal handler blocks until the script exits, so the script should complete quickly. Heavy processing (uploading the dump, symbolization, alerting) should be delegated to background processes that the script spawns before exiting.

{% endnote %}

Example of a script that only moves the dump to an archive directory:

```bash
#!/bin/sh
# $1 — succeeded (true|false), $2 — path to the minidump
if [ "$1" = "true" ]; then
    mv "$2" /var/log/ydb/minidumps/archive/
fi
```

## Analyzing minidumps {#analyze}

A minidump does not contain symbols: to obtain a human-readable stack, you need the symbols of the same `ydbd` build artifact. The standard Google Breakpad tools (`minidump_stackwalk` and `.sym` files) produced from the symbols of the corresponding build are used for the analysis.

## See also {#see-also}

- [{#T}](logging.md)
- [{#T}](monitoring.md)
- [Google Breakpad](https://chromium.googlesource.com/breakpad/breakpad/)
