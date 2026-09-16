# Path rewrite rules

`TPathNormalizer` is the shared matcher for logical YDB schema paths. It does not
replace lexical path canonicalization or resource validation.

Configure aliases in the startup `ydbd` YAML configuration:

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

The server creates the immutable matcher before actors start and shares it across
requests. Configuration changes require a restart; changing a live console
configuration does not replace an existing node's matcher. Invalid expressions
or replacement references prevent startup.
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

## Request semantics

Discovery's database operand and request database headers are resolved before
routing and authorization. Each resource-owning handler resolves its complete
logical operand once. SQL text and paths inside SQL are not rewritten. Queries
continue to use their existing path-resolution rules under the effective
database; absolute SQL paths must name the physical resource. Object names local
to a service (such as consumers and semaphores), remote references, and Federated
Query cloud-folder IDs and binding names remain unchanged.

Aliases preserve each API's existing path grammar. For example, Topic APIs
accept database-relative paths, while native table-creation APIs still
require their usual root-relative spelling. Backup collections retain their
special `.backups/collections` namespace. A complete logical candidate is matched
before target database containment, scheme validation, and ACL checks. Rewrites
do not grant permissions or bypass tenant boundaries.

A changed target must be an absolute, nonempty schema path without NUL bytes or
`.`/`..` components. Repeated separators are canonicalized by the owning adapter.
Missing and empty request operands retain their existing validation behavior,
even when a rule matches an empty string. Identity matches stop rule processing.

Internal forwarding retains server-owned provenance: resolved paths are not
matched again. Continuation tokens keep the physical identity of the operation
that created them; every continuation still performs its existing authorization
checks. Protocol correlation names remain logical when clients must reuse them
in later messages.

## Restart and persisted work

Upgrade all participating nodes, including SchemeShard nodes, before enabling
rules, and deploy identical rules throughout the cluster. SQL compilation,
query caches, scripts, views, and streaming-query definitions are unchanged.

Imports resolve complete manifest destinations atomically before entering their
execution phase. Rules must remain unchanged until that resolution finishes.
Once destinations are persisted as physical paths, a resumed import uses those
paths without applying new rules. Export URLs and import filesystem paths are
never rewritten.

## Performance

Expressions are compiled once. RE2 avoids exponential backtracking, but rules
are checked in order: a miss or late match still examines all preceding rules.
Simple anchored prefixes benefit from RE2's prefix checks; this does not make
an arbitrarily large rule set constant-time. Returning a path also copies its
bytes. Callers should bypass the matcher when `Empty()` is true.

The `benchmark` target measures construction and first/last/no-match cases for
0, 1, 10, 100, 1000, and 10000 prefix or complex rules, with short and 64 KiB paths.
RSS counters are process-wide observations, not exact allocation measurements.
