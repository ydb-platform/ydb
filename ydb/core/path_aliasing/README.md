# Path rewrite rules

`TPathNormalizer` is the shared matcher for logical YDB schema paths. It does not
replace lexical path canonicalization or resource validation.

This is the matcher and configuration-schema foundation only. Request routing,
discovery, and SQL integration are not implemented yet; adding the configuration
below does not currently redirect server requests.

```yaml
path_rewrite_config:
  rules:
    - pattern: '^/kfront(/|$)'
      replacement: '/failover/kfront\1'
```

Rules use RE2 syntax and are evaluated in configuration order. The first matching
rule replaces its first match, preserving the unmatched prefix and suffix. A
matching identity rule also stops processing. Results are never recursively
rewritten, so overlapping rules and cycles are well-defined. Explicit empty
patterns and replacements are allowed; omitted fields and invalid expressions
or replacement references are rejected when the configuration is compiled.

Use YAML single quotes to preserve replacement backslashes. `\0` references the
whole match and `\1` through `\9` reference capture groups. Omitting the section
or supplying no rules disables rewriting. There is no fixed rule-count limit.

Server integration must create the immutable matcher before actors start and
share it across requests. The configuration is intended to be restart-only.
Its fingerprint identifies the ordered rules and their semantics; an empty rule
set has an empty fingerprint.

Callers must form the complete logical path before applying the matcher, retain
the resolved path across internal forwarding, and apply existing validation and
authorization to the resolved target. A relative path must use its original
logical database, not the already rewritten database. Reapplying normalization
to a resolved path is incorrect even if common prefix rules appear idempotent.

Only local YDB schema paths are eligible. Remote database references, URLs,
filesystem paths, SQL values, and service-local resource identifiers are not
schema paths and must not be passed to this matcher.

## Performance

Expressions are compiled once. RE2 avoids exponential backtracking, but rules
are checked in order: a miss or late match still examines all preceding rules.
Simple anchored prefixes benefit from RE2's prefix checks; this does not make
an arbitrarily large rule set constant-time. Returning a path also copies its
bytes. Callers should bypass the matcher when `Empty()` is true.

The `benchmark` target measures construction and first/last/no-match cases for
0, 1, 10, 100, 1000, and 10000 prefix or complex rules, with short and 64 KiB paths.
RSS counters are process-wide observations, not exact allocation measurements.
