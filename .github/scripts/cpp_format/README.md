# C++ style checks

C++ formatting is checked through Ya's `STYLE_CPP` tests and the existing CI
pipeline. Directories opt in through autoincludes, and multiple modules or teams
can reference a single clang-format configuration.

## Team configuration

The enabled scope is defined by
[autoincludes.json](../../../build/internal/conf/autoincludes.json) and each
directory's `linters.make.inc`. Coverage applies to the listed subtrees.

| Team | Enabled scope | Style | Configuration |
| --- | --- | --- | --- |
| CS (`@ydb-platform/cs`) | `ydb/core/tx/columnshard` | Common | [.github/config/cpp_format/.clang-format](../../config/cpp_format/.clang-format) |
| NBS (`@ydb-platform/nbs_yandex`) | `ydb/core/nbs` | Own | [ydb/core/nbs/.clang-format](../../../ydb/core/nbs/.clang-format) |
| FQ (`@ydb-platform/fq`) | Not enabled yet; rollout per directory | Common (planned) | [.github/config/cpp_format/.clang-format](../../config/cpp_format/.clang-format) |

Update this table when enabling a team's directories or changing their style
configuration. The examples below show CS's existing setup and how to enable an
FQ directory with the common style.

## Configuration

Three files define the setup:

| File | Purpose |
| --- | --- |
| [build/internal/conf/autoincludes.json](../../../build/internal/conf/autoincludes.json) | Registers directories whose configuration is included for modules in their subtrees. |
| `<directory>/linters.make.inc` | Enables style checks and selects their configuration. |
| [.github/config/cpp_format/.clang-format](../../config/cpp_format/.clang-format) | Common formatting rules that modules and teams can share. |

For example, CS uses this
[linters.make.inc](../../../ydb/core/tx/columnshard/linters.make.inc):

```text
IF (MODULE_LANG == CPP)
    SET(MODULE_COMMON_CONFIGS_DIR .github/config/cpp_format)
    STYLE_CPP(CONFIG_TYPE .clang-format)
ENDIF()
```

The guard applies the check to C++ modules. `MODULE_COMMON_CONFIGS_DIR` is a path
relative to the repository root. `CONFIG_TYPE .clang-format` selects the supported
configuration filename within that directory; the directory path belongs in
`SET`.

A directory can use its own `.clang-format` by omitting `SET`; Ya then uses the
configuration in the autoinclude root. The local formatting helper respects each
directory's declared configuration.

## Enable checks for a directory

Run the commands below from the repository root. The example uses
`ydb/core/external_sources`, an FQ directory that can adopt the shared style.

1. Add the directory to
   [autoincludes.json](../../../build/internal/conf/autoincludes.json), preserving
   existing entries. For example:

   ```json
   [
       "ydb/core/nbs",
       "ydb/core/tx/columnshard",
       "ydb/core/external_sources"
   ]
   ```

2. Create `ydb/core/external_sources/linters.make.inc` using the shared-config
   template above.

3. Preview and format the selected files:

   ```bash
   python3 .github/scripts/cpp_format/format.py --list --root ydb/core/external_sources
   python3 .github/scripts/cpp_format/format.py --fix --root ydb/core/external_sources
   ```

4. Run the native Ya style tests:

   ```bash
   set -o pipefail
   ./ya make --build relwithdebinfo -tA --style ydb/core/external_sources 2>&1 | tail -60
   ```

The formatting commands require the autoinclude entry and `linters.make.inc` to
be in place. Repeat this process for each directory being enabled; all directories
using the shared configuration get the same formatting rules.

Choose a root containing the relevant Ya modules. If a folder's sources belong
to a module declared in a parent directory, configure that containing module.
An autoinclude in the source-only subfolder does not enable checks in its parent.

## Check scope in CI and locally

CI selects affected tests using the build graph. Each selected `STYLE_CPP` test
checks its module's registered C/C++ sources and headers, subject to Ya's style
exclusions. The check covers whole files, including unchanged files in that
module. Enabling checks on a module can therefore expose existing formatting
differences.

The local helper scans eligible tracked files recursively under the enabled
autoinclude roots. It also includes sources that are absent from module source
lists. Use the native Ya command above to validate the module's actual style
tests.

| Command | Scope |
| --- | --- |
| `./ya make ... -tA --style <directory>` | Style tests for the selected build targets. |
| `format.py --check` | Eligible tracked files under every formatting-enabled autoinclude root. |
| `format.py --check --root <directory>` | Eligible tracked files under one enabled root. |

## Local formatting helper

[format.py](format.py) uses Python's standard library. It discovers roots from
`build/internal/conf/autoincludes.json` and selects those with
`STYLE_CPP(CONFIG_TYPE .clang-format)` in their `linters.make.inc`.

```bash
# List selected files.
python3 .github/scripts/cpp_format/format.py --list

# Check all enabled roots without editing files.
python3 .github/scripts/cpp_format/format.py --check

# Apply formatting to all enabled roots.
python3 .github/scripts/cpp_format/format.py --fix

# Limit an operation to one enabled root, such as CS.
python3 .github/scripts/cpp_format/format.py --fix --root ydb/core/tx/columnshard
```

`--root` takes an exact autoinclude root relative to the repository and can be
repeated. Newly configured FQ or other directories are discovered automatically.

The helper supports the configuration template above, with or without the
`MODULE_LANG == CPP` guard or the `SET` line. It rejects other formatting
constructs and overlapping autoinclude roots before editing files. It does not
evaluate arbitrary ymake code or module-specific settings.

### File selection and exclusions

The helper uses the source and header extensions defined by
[`STYLE_CPP`](../../../build/plugins/lib/test_const/__init__.py). It includes
staged additions; stage new source files to include them in a run. Deleted files,
symlinks, and untracked build outputs are excluded.

It mirrors the common Ya exclusions used by the
[native checker](../../../tools/cpp_style_checker/wrapper.py):

- Paths containing `contrib`, `vendor`, or `generated` directory components.
  Ya's exceptions for `devtools/contrib` and contrib `.yandex_meta` files are
  preserved.
- Files containing `# DO_NOT_STYLE` or `// DO_NOT_STYLE`.
- Files containing the license markers `THIS SOFTWARE` or `WITHOUT WARRANT`.

For example, vendored files under `ydb/core/nbs/cloud/contrib/vhost` are excluded
when checking NBS.

### Formatter and exit status

The helper resolves `./ya tool clang-format-18 --print-path`, using the same
formatter resource as Ya's style tests. `--list` does not resolve or download the
formatter. To reuse a previously resolved Ya binary offline, set
`YDB_CLANG_FORMAT` to its executable path.

Exit status is 0 on success, 1 on formatting failures, and 2 for configuration or
invocation errors.

CS also provides
[`scripts/format-all.sh`](../../../ydb/core/tx/columnshard/scripts/format-all.sh),
which calls this helper with the CS root selected. It applies formatting by
default and accepts `--list` or `--check`.

## Editor configuration

Editors that discover clang-format configuration through parent directories need
a `.clang-format` in the enabled subtree. A relative symlink to the shared file
keeps the rules in one place. CS's
[`.clang-format`](../../../ydb/core/tx/columnshard/.clang-format) demonstrates
this setup.

For the FQ example, create the link from the repository root:

```bash
ln -s ../../../.github/config/cpp_format/.clang-format ydb/core/external_sources/.clang-format
```

The Ya include should still reference the shared directory directly. If the
editor supports an external clang-format executable, use the path printed by:

```bash
python3 .github/scripts/cpp_format/format.py --print-binary
```

Select that formatter for C++ files in the editor, then use its format-document
action or enable formatting on save.

## Helper tests

```bash
set -o pipefail
python3 -m unittest discover -s .github/scripts/cpp_format/tests -v 2>&1 | tail -40
```

Set `YDB_CLANG_FORMAT` to a resolved Ya formatter path to include the real
check/fix tests. Those tests format temporary fixtures.
