# Feature flag defaults for documentation builds

`generate.py` reads protobuf sources from the checked-out repository, compiles a descriptor set, validates flag references in documentation, and adds a generated block to the `default` preset in `ydb/docs/presets.yaml`. PR previews and release builds run it before Diplodoc. The local `ydb/docs/build.sh` entry point runs it too.

Install the pinned dependencies and run:

```bash
python3 -m pip install -r ydb/docs/tools/feature_flags/requirements.txt
python3 -m unittest discover -s ydb/docs/tools/feature_flags -p 'test_*.py'
python3 ydb/docs/tools/feature_flags/generate.py --catalog feature-flag-defaults.json
```

Run these commands from the repository root. For another documentation root, pass `--docs-root <path>`. The generator never fetches source files from another branch or from the network. Do not commit the generated presets block or edit its values manually. It is regenerated on each build, preserving existing presets and comments.

## Variable names and semantics

| Source | Variable | Value |
| --- | --- | --- |
| `NKikimrConfig.TFeatureFlags` | `feature_flags.enable_xxx` | Protobuf boolean default; supported `Tribool` values become `true`, `false`, or `null` for `UNSET`. |
| Other protobuf messages | `proto_flags.<package>.<message>.<snake_case_field>` | Boolean schema default or the symbolic `Ydb.FeatureFlag.Status` default. |
| Registered actor C++ structs | `compile_time_flags.<namespace>.<struct>.<snake_case_field>` | Literal `constexpr bool`; other expressions are `null`. |

For example:

```markdown
{% if feature_flags.enable_xxx == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default. [Learn more](../reference/configuration/feature_flags.md#experimental).

{% endnote %}
{% endif %}
```

Replace `enable_xxx` with the actual flag and adjust the relative link for the page's location. Add the warning to every page documenting that experimental feature. A flag disabled by default does not automatically make the feature experimental: authors add these conditions only to functionality that is experimental.

A service-config flag uses its package and message hierarchy as a qualified key:

```markdown
{% if proto_flags.NKikimrConfig.TTableServiceConfig.enable_stream_write == false %}
<!-- Add the appropriate warning here. -->
{% endif %}
```

Do not flatten other messages into `feature_flags`: identical field names can have different defaults and meanings. Public API `STATUS_UNSPECIFIED` is not `false`, a Viewer snapshot is not a cluster configuration setting, and physical-query fields describe query-plan data. The JSON inventory labels these categories explicitly. Their schema defaults do not establish whether an experimental cluster feature is available. C++ literals describe source defaults, not every possible binary build configuration. Generated values do not reflect live cluster overrides or defaults assigned by application code.

## Source coverage

`sources.json` lists the reviewed source files, FQ source globs, and actor structs. It covers the canonical flags, table service, AppConfig, PQ, scheme operations, Blob Storage, authentication, KQP physical/runtime messages, FQ, public API, Viewer, and NBS ddisk. Imported YDB protobufs are included too, so configurations delegated to other message types are covered. All singular boolean fields in these schemas are inventoried, including names that do not start with `Enable`. Map entries and repeated fields are excluded.

The inventory is intentionally broader than the set of experimental features. Register another root schema or C++ struct in `sources.json` when it is not reachable through the current protobuf imports. CI triggers on protobuf changes as well as documentation and registered C++ header changes.

Older release branches may lack sources added in newer releases. Those are listed as absent in the inventory, rather than read from `main`. The canonical schema is required. A reference to an absent field or message fails validation.

## Deleted flags and spelling checks

Reserve the original protobuf field name when removing a flag. Its generated value becomes `null`, so a condition comparing it with `false` stops displaying the warning. Unknown names still fail the build; a typo cannot silently hide a warning. For flags whose original name cannot be reserved (including removed C++ fields or entire messages), record the qualified reference in `retired_references` in `sources.json`. Such entries must not name an active field.

Flag references must use static keys in the forms shown above. Validation skips fenced Markdown examples, where placeholder names such as `enable_xxx` are expected. Code implementing a feature's own default calculation is not evaluated: the inventory records protobuf schema defaults and supported C++ literals only.
