---
title: "Overview of the GROUP BY operator in {{ ydb-full-name }}"
description: "The article describes which functions are used with the GROUP BY operator in {{ ydb-full-name }}. We review examples of queries using the GROUP BY operator."
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


