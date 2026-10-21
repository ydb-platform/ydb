# Versioning {{ ydb-short-name }}

Releases of {{ ydb-short-name }} are named with a version string consisting of several components. Depending on the context, some of the rightmost components may be omitted.

Ordered list of components:

1. The last two digits of the release year
2. The sequential number of the major version within the year
3. The sequential number of the minor version within the major version
4. The patch number within the minor version
5. The release type, for example `testing`, `stable`, or `lts`.

The first three components form the base version of the release, for example `25.2.1`. The fourth component and the type specify the particular build, for example `25.1.4.7` or `25.1.4.7-stable`. The patch number and type do not affect version compatibility rules.

Thus, major releases are usually identified by two components, for example `25.1`. Minor releases are identified by three components, for example `25.1.4`. The full version includes the patch number and, if necessary, the release type.

The list of available versions can be obtained on the [download page](../../downloads/index.md). The release policy of {{ ydb-short-name }} is described in more detail in the article [Release Management](../../contributor/manage-releases.md) in the developer documentation section {{ ydb-short-name }}. The release branch scheme is in the [{#T}](../../contributor/manage-releases.md#release_branch_scheme) section.

## Version compatibility {#version-compatability}

All minor versions within a single major version are compatible for update. Major versions are compatible sequentially. To update to the next major version, you should first update to the latest available minor release of the current major version. For example:

* `X.Y.*.* → X.Y.*.*` — update is possible, all minor versions within a single major version are compatible.
* `X.Y.Z.*` (latest available `X.Y.*.*`) → `X.Y+1.*.*` — update is possible, major versions are sequential.
* `X.Y.*.*` → `X.Y+2.*.*` — update is impossible, major versions are not sequential.
* `X.Y.*.* → X.Y-2.*.*` — update is impossible, major versions are not sequential.

{% note warning %}

Also, in any case, you cannot roll back more than 2 major versions from a version that has been installed at least once, because such an old version may not know how to work with data on disks written by the current version.

{% endnote %}

### Version compatibility examples

* `v.25.1.3.2` -> `v.25.1.5.5` — update is possible
* `v.25.1.5.5` -> `v.25.2.3.1` — update is possible (where `v25.1.5.*` is the latest available minor version in `v.25.1`)
* `v.25.1.4.1` -> `v.25.2.3.1` — update is impossible, you must first update to the latest minor version (`v.25.1.5.*`)
* `v.25.1.5.5` -> `v.25.3.5.3` — update is impossible, you must first update to the next major version (`v.25.2.*.*`).

## Formal description of possible versions


```bnf
<valid-version> ::= <version-core> "-" <version-type>

<version-core> ::= <year> "." <major> "." <minor> "." <patch>

<version-type> ::= "testing" | "stable" | "lts"

<year> ::= <positive digit> <digit>

<major> ::= <positive digit>

<minor> ::= <digits>

<patch> ::= <digits>

<digit> ::= "0" | <positive digit>

<positive digit> ::= "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<digits> ::= <digit> | <digit> <digits>
```


### Examples of full versions

* Test version: `25.1.4.7-testing`
* Stable version: `25.1.4.7-stable`
* LTS version: `25.1.4.7-lts`
