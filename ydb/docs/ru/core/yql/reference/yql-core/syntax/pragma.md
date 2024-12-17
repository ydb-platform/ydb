# PRAGMA

{% include [x](_includes/pragma/definition.md) %}

{% include [x](_includes/pragma/global.md) %}

{% include [x](_includes/pragma/yson.md) %}

{% include [x](_includes/pragma/files.md) %}

{% if backend_name == "YDB" %}

{% include [x](_includes/pragma/ydb.md) %}

{% endif %}

{% include [x](_includes/pragma/debug.md) %}
