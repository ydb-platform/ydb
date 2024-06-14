# Downloads

## {{ ydb-short-name }} CLI {#ydb-cli}

[{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) is a command line utility for working with {{ ydb-short-name }} databases.

{% list tabs %}

- Linux

   {% include notitle [Linux](_includes/ydb-cli/linux.md) %}

- macOS (Intel)

   {% include notitle [macIntel](_includes/ydb-cli/darwin_amd64.md) %}

- macOS (M1 arm)

   {% include notitle [macM1](_includes/ydb-cli/darwin_arm64.md) %}

- Windows

   {% include notitle [Windows](_includes/ydb-cli/windows.md) %}

{% endlist %}

## {{ ydb-short-name }} Server {#ydb-server}

{{ ydb-short-name }} Server is a build for running a [YDB cluster](../concepts/glossary.md#cluster) node.

{% list tabs %}

- Linux

   {% include notitle [linux](_includes/server/linux.md) %}

- Docker

   {% include notitle [docker](_includes/server/docker.md) %}

- Source code

   {% include notitle [docker](_includes/server/source_code.md) %}

{% endlist %}

## {{ ydb-short-name }} DSTool {#ydb-dstool}

{{ ydb-short-name }} DSTool is a command line utility for [managing a {{ ydb-short-name }} cluster's disk subsystem](../maintenance/manual/index.md).

To use the utility, install the [ydb-dstool](https://pypi.org/project/ydb-dstool/) Python package.
