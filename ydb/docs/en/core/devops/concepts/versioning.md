# {{ ydb-short-name }} Versioning

{{ ydb-short-name }} releases are named with a version that is a string consisting of several components. Depending on the context, some of the rightmost components may be omitted. The ordered list of components:

1. Last two digits of the release year
2. Major version number within the year
3. Minor version number within the major version
4. Patch number within the minor version
5. Release type

Major releases are usually identified by two components, for example, `24.3` is the third major release in 2024. Minor releases are identified by three components, for example, `24.3.14` is the fourteenth minor release within the third major version of 2024.

The list of available {{ ydb-short-name }} releases is published on the [download page](../../downloads/index.md). The {{ ydb-short-name }} release policy is described in detail in the [{#T}](../../contributor/manage-releases.md) article in the documentation section for {{ ydb-short-name }} developers.

## Version Compatibility {#version-compatibility}

{% note info %}

Previously, the {{ ydb-short-name }} server version consisted of three numbers (for example, `v24.3.3`), starting with major version `25.1`. The fourth number was added to specify the patch number (for example, `v25.1.1.3`). For more details about changes in version naming, see [{#T}](../../contributor/manage-releases.md).

{% endnote %}

You can update {{ ydb-short-name }} to a new version in the following cases:

- If the major version numbers are the same, the minor versions can differ.
- If the major version numbers differ by one, first update to the latest available minor release of the current major version before updating to the next major version.

For example:

* `X.Y.*.* → X.Y.*.*` — update is possible, all minor versions within the same major version are compatible.
* `X.Y.Z.*` (last available `X.Y.*.*`) → `X.Y+1.*.*` - update is possible, major versions are sequential.
* `X.Y.*.*` → `X.Y+2.*.*` — update is not possible, major versions are non-sequential.
* `X.Y.*.* → X.Y-2.*.*` — update is not possible, major versions are non-sequential.

{% note warning %}

Also, you cannot downgrade {{ ydb-short-name }} by more than two major versions because the older version might not support newer data formats stored on disk.

{% endnote %}

### Version Compatibility Examples

* `v.25.1.3.2`  →  `v.25.1.5.5` - update is possible
* `v.25.1.5.5`  →  `v.25.2.3.1` - update is possible (where `v25.1.5.*` is the last available minor version in `v.25.1`)
* `v.25.1.4.1`  →  `v.25.2.3.1` - update is not possible, you must first update to the last minor version (`v.25.1.5.*`)
* `v.25.1.5.5`  →  `v.25.3.5.3` - update is not possible, you must first update to the next major version (`v.25.2.*.*`).

## Formal Description of Possible Versions

```bnf
<valid-version> ::= <version-core> "-" <version-type>
                  | <version-core>

<version-core> ::= <year> "." <major>
                 | <year> "." <major> "." <minor>
                 | <year> "." <major> "." <minor> "." <patch>

<year> ::= <digit> <digit>

<major> ::= <digits>

<minor> ::= <digits>

<patch> ::= <digits>

<version-type> ::= "testing"
                 | "stable"
                 | "lts"

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<digits> ::= <digit> | <digit> <digits>
```

### Examples of Full Versions

* Testing version: `24.3.13.6-testing`
* Stable version: `24.3.14.2-stable`
* LTS version: `24.3.14.2-lts`