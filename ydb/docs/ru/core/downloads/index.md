# Загрузки

## {{ ydb-short-name }} CLI {#ydb-cli}

[{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) — утилита командной строки для работы с базами данных {{ ydb-short-name }}.

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

{{ ydb-short-name }} Server — сборка для запуска узла [кластера YDB](../concepts/glossary.md#cluster).

{% list tabs %}

- Linux

  {% include notitle [linux](_includes/server/linux.md) %}

- Docker

  {% include notitle [docker](_includes/server/docker.md) %}

- Исходный код

  {% include notitle [docker](_includes/server/source_code.md) %}

{% endlist %}

## {{ ydb-short-name }} DSTool {#ydb-dstool}

{{ ydb-short-name }} DSTool — утилита командной строки для [управления дисковой подсистемой](../maintenance/manual/index.md) кластера {{ ydb-short-name }}.

Для использования утилиты установите Python-пакет [ydb-dstool](https://pypi.org/project/ydb-dstool/).
