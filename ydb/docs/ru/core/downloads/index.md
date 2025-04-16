# Загрузки

## {{ ydb-short-name }} CLI {#ydb-cli}

[{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) — утилита командной строки для работы с базами данных {{ ydb-short-name }}.

{% list tabs group=os %}

- Linux (amd64)

  {% include notitle [LinuxAmd64](_includes/ydb-cli/linux_amd64.md) %}

- Linux (arm64)

  {% include notitle [LinuxArm64](_includes/ydb-cli/linux_arm64.md) %}

- MacOS (amd64)

  {% include notitle [macIntel](_includes/ydb-cli/darwin_amd64.md) %}

- MacOS (arm64)

  {% include notitle [macM1](_includes/ydb-cli/darwin_arm64.md) %}

- Windows

  {% include notitle [Windows](_includes/ydb-cli/windows.md) %}

{% endlist %}

## {{ ydb-short-name }} Server {#ydb-server}

{{ ydb-short-name }} Server — сборка для запуска узла [кластера YDB](../concepts/glossary.md#cluster).

{% list tabs group=os %}

- Linux

  {% include notitle [linux](_includes/server/linux.md) %}

- Docker

  {% include notitle [docker](_includes/server/docker.md) %}

- Исходный код

  {% include notitle [source_code](_includes/server/source_code.md) %}

{% endlist %}

## {{ ydb-short-name }} DSTool {#ydb-dstool}

{{ ydb-short-name }} DSTool — утилита командной строки для [управления дисковой подсистемой](../maintenance/manual/index.md) кластера {{ ydb-short-name }}.

Для использования утилиты установите [ydb-dstool](../reference/ydb-dstool/install.md).

## {{ ydb-short-name }} Ops {#ydbops}

{{ ydb-short-name }} Ops — утилита командной строки для [управления кластером {{ ydb-short-name }}](../reference/ydbops/index.md).

{% list tabs group=os %}

- Linux

  {% include notitle [linux](_includes/ydbops/linux.md) %}

- macOS (Intel)

  {% include notitle [linux](_includes/ydbops/darwin_amd64.md) %}

- macOS (M1 Arm)

  {% include notitle [linux](_includes/ydbops/darwin_arm64.md) %}


{% endlist %}
