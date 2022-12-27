---
title: "Обзор оператора GROUP BY в {{ ydb-full-name }}"
description: "В статье рассказываем о том, какие функции применяются совместно с оператором GROUP BY в {{ ydb-full-name }}. Рассматриваем примеры запросов с использованием оператора GROUP BY."
---

{% if select_command == "SELECT STREAM" %}

  {% include [x](_includes/group_by/general_stream.md) %}

  {% include [x](_includes/group_by/having_stream.md) %}

{% else %}

{% include [x](_includes/group_by/general.md) %}

{% include [x](_includes/group_by/session_window.md) %}

{% if feature_group_by_rollup_cube %}

  {% include [x](_includes/group_by/rollup_cube_sets.md) %}

{% endif %}

{% include [x](_includes/group_by/distinct.md) %}

{% include [x](_includes/group_by/compact.md) %}

{% include [x](_includes/group_by/having.md) %}

{% endif %}


