# SDK snippet preparation

SDK examples live in each SDK repository under `ydb_tech`. The documentation
manifest selects repositories, refs and allowed paths. The lock records the exact
commit and, for each selected file, its Git blob SHA, SHA-256 and byte size.

The updater traverses only the requested GitHub subtrees and downloads matching
files individually from the locked commit. It does not download repository archives.
Preparation uses the lock without following moving branch heads.

From the YDB repository root:

```sh
python -m pip install -r ydb/docs/tools/sdk_snippets/requirements.txt
python ydb/docs/tools/sdk-snippets prepare
python ydb/docs/tools/sdk-snippets validate
```

The same preparation action runs before PR and release documentation builds.
SDK examples execute in their own repository CI. Documentation CI treats downloaded
source as text and validates all referenced files and named regions.

Run preparation before `./ya make ydb/docs` as well: the `DOCS()` target copies
the prepared source files into its isolated build input. Native CI prepares them
before creating build graphs, refreshing the staging directory against each
commit's lock when the incremental comparison switches between base and HEAD.

## Update one SDK

```sh
python ydb/docs/tools/sdk-snippets update go --kind branch --ref docs/topic-snippets
python ydb/docs/tools/sdk-snippets prepare
```

After an SDK release contains the examples, select its tag:

```sh
python ydb/docs/tools/sdk-snippets update go --kind tag --ref "$SDK_RELEASE_TAG"
```

Set `SDK_RELEASE_TAG` to the actual release containing the required regions. `update` resolves annotated
tags to commits and rejects a moved tag that was already locked. Review and commit
the manifest and lock together. Normal builds never update the lock.

## Select files and regions

`include` accepts exact paths and glob patterns within `examples/ydb_tech/` (or `ydb/examples/ydb_tech/` for Rust). Choose the
smallest set required by the documentation. A missing path, empty match, symlink,
submodule, oversized file or truncated GitHub tree fails the update.

Both documentation languages use the same root-relative path and region:

```markdown
{% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_create]-[END topic_create]" %}
```

Regions use Latin letters, digits and underscores. They must be unique within a
file, nonempty, paired and free of overlaps or nesting. `lang` is required.
Validation runs before Diplodoc, so missing markers cannot use its fallback behavior.

## Cache and offline builds

```sh
python ydb/docs/tools/sdk-snippets --cache /tmp/sdk-snippets-cache prepare
python ydb/docs/tools/sdk-snippets --cache /tmp/sdk-snippets-cache prepare --offline
```

Files are cached by SHA-256 and checked against both locked digests before use.
CI restores this cache by lock hash. Missing offline files and corrupt cached
files fail explicitly. Set `GH_TOKEN` or `GITHUB_TOKEN` to use authenticated GitHub
API rate limits; tokens are never used in raw source URLs or written to the lock.

Preparation replaces `.generated/sdk-snippets` only after all files and regions
validate. The generated directory is ignored by Git and excluded from published output.

## Compare snippets

```sh
git show main:ydb/docs/sdk-snippets.lock.yaml > /tmp/sdk-snippets-before.yaml
python ydb/docs/tools/sdk-snippets diff --before /tmp/sdk-snippets-before.yaml
```

CI publishes the resulting content diff as an artifact. The source update and
strict validation commands are independent of the documentation renderer.
