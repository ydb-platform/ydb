# Manage {{ ydb-short-name }} releases

There are two products based on the source code from the [{{ ydb-short-name }} repository](https://github.com/ydb-platform/ydb) with independent release cycles:

- [{{ ydb-short-name }} server](#server)
- [{{ ydb-short-name }} command-line interface (CLI)](#cli)

## {{ ydb-short-name }} server release cycle {#server}

### Release numbers and schedule {#server-versioning}

{{ ydb-short-name }} server version consists of three numbers separated by dots:

1. The last two digits of calendar year of the release
2. Major release ordinal number in a given year
3. Minor release ordinal number for a given major release

Thus, {{ ydb-short-name }} server major version is a combination of the first two numbers (for example, `23.3`), and the full version is a combination of all three (for example, `23.3.5`).

{{ ydb-short-name }} server release schedule typically includes 4 major releases per year, so the release `YY.1` is the first one, and `YY.4` is the last one for a year `YY`. The number of minor releases is not fixed, and may vary from one major release to another.

### Compatibility {#server-compatibility}

{{ ydb-short-name }} maintains compatibility between major versions to ensure a cluster can operate while its nodes run two adjacent major versions of the YDB server executable. You may refer the [Updating {{ ydb-short-name }}](../devops/manual/upgrade.md) article to learn more about the cluster upgrade procedure.

Given the above compatibility target, major releases go in pairs: odd numbered releases add new functionality switched off by feature flags, and even numbered releases enable that functionality by default.

For instance, release `23.1` comes with the new functionality switched off. It can be incrementally rolled out to a cluster running `22.4`, without downtime. As soon as the whole cluster runs `23.1` nodes, you can manually toggle feature flags to test new functionality and later further upgrade it to `23.2` to fully leverage this new functionality.

### Release branches and tags {#server-branches-tags}

A release cycle for an odd major release starts by a member of a [YDB Release team](https://github.com/orgs/ydb-platform/teams/release) forking a new branch from the `main` branch. Major release branch name starts with prefix `stable-`, followed with the major version with dots replaced by dashes (for example, `stable-23-1`).

A release cycle for an even major release starts by branching from the preceding odd major version branch. The branch follows the same naming convention.

All major version releases, both odd and even, go through the comprehensive testing process producing a number of minor versions. Each minor version is created by tagging a relevant commit of the release branch with a full version number. So, there can be tags `24.1.1`, `24.1.2` etc. on the `stable-24-1` branch. As soon as a minor version proves its quality, we consider it as stable, and register a Release on GitHub linked to its tag, add it to [downloads](../downloads/index.md#ydb-server) and [changelog](../changelog-server.md) documentation pages, etc. Thus, there can be more that one stable release for a major version.

### Testing {#server-testing}

Release testing is iterative. Each iteration starts by assigning a tag to a commit on the release branch, specifying a minor version to be tested. For example, the minor version tag `23.3.5` marks a 5th testing iteration for the major release `23.3`.

A tag can be considered to be either "candidate" or "stable". Initially, the first tag is created in the release branch right after its creation. This tag is considered as "candidate".

During a testing iteration, code from release branches undergoes an extensive testing including deployment on [UAT](https://en.wikipedia.org/wiki/Acceptance_testing), prestable, and production environments of companies using YDB. To perform such testing, YDB code from a GitHub release tag is imported into the corporate context of a given user, following its internal policies and standards. Then it's built, deployed to the necessary environments, and thoroughly tested.

{% include [corp_release_testing.md](_includes/corp_release_testing.md) %}

Based on a list of uncovered problems, the [YDB Release team](https://github.com/orgs/ydb-platform/teams/release) decides if the current minor release can be promoted to be called "stable", or a new testing iteration must be started over with a new minor release tag. In fact, as soon as a critical problem is discovered during testing, developers fix it in the `main` branch, and backport changes to the release branch right away. So, by the time testing iteration finishes, there will be a new tag and a new testing iteration if there are some new commits on top of the current tag.

### Stable release {#server-stable}

If testing iteration proves the quality of a minor release, the [YDB Release team](https://github.com/orgs/ydb-platform/teams/release) prepares the [release notes](../changelog-server.md), and publishes the YDB server release on both the [GitHub Releases](https://github.com/ydb-platform/ydb/releases) and [Downloads](../downloads/index.md#ydb-server) pages, therefore declaring it as "stable".

{% include [corp_release_stable.md](_includes/corp_release_stable.md) %}

## {{ ydb-short-name }} CLI (Command-Line Interface) release cycle {#cli}

### Release numbers and schedule {#cli-versioning}

{{ ydb-short-name }} CLI version consists of three numbers separated by dots:

1. Major release ordinal number (currently, `2`)
2. Minor release ordinal number for a given major release
3. Patch number

For example, `2.8.0` is the 2nd major release, 8th minor, without additional patches.

There's no schedule for the {{ ydb-short-name }} CLI minor releases, a new release comes as soon as there's some new valuable functionality. Initially, every new minor release has `0` as a patch number. If there are critical bugs found in that version, or some minor part of functionality did not catch it as planned, a patch can be released, incrementing only the patch number, like it was for `2.1.1`.

In general, release cycle for {{ ydb-short-name }} CLI is much simpler and shorter than for the server, producing more frequent releases.

### Release tags {#cli-tags}

Tags for {{ ydb-short-name }} CLI are assigned on the `main` branch by a member of the [{{ ydb-short-name }} Release team](https://github.com/orgs/ydb-platform/teams/release) after running tests for some revision. To distinguish from the {{ ydb-short-name }} server tags, {{ ydb-short-name }} CLI tags have a `CLI_` prefix before the version number, for example [CLI_2.8.0](https://github.com/ydb-platform/ydb/tree/CLI_2.8.0).

{% include [corp_cli_tags.md](_includes/corp_cli_tags.md) %}

### Stable release {#cli-stable}

To declare a {{ ydb-short-name }} CLI tag as stable, a member of the [YDB Release team](https://github.com/orgs/ydb-platform/teams/release) prepares the [release notes](../changelog-cli.md), and publishes the release on the [GitHub Releases](https://github.com/ydb-platform/ydb/releases) and [Downloads](../downloads/index.md#ydb-cli) pages.