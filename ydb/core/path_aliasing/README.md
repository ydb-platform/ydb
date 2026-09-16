# Path rewrite rules

`TPathNormalizer` is the shared matcher for logical YDB schema paths. It does not
replace lexical path canonicalization or resource validation.

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

The immutable matcher is created before actors start and shared by requests.
Changing rules requires a node restart. Its fingerprint identifies the ordered
rules and their semantics; an empty rule set has an empty fingerprint.

Callers must form the complete logical path before applying the matcher, retain
the resolved path across internal forwarding, and apply existing validation and
authorization to the resolved target. A relative path must use its original
logical database, not the already rewritten database. Reapplying normalization
to a resolved path is incorrect even if common prefix rules appear idempotent.

Only local YDB schema paths are eligible. Remote database references, URLs,
filesystem paths, SQL values, and service-local resource identifiers are not
schema paths and must not be passed to this matcher.
