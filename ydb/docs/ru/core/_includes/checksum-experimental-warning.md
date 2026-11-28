{% if feature_flags.enable_checksum_table == false %}
{% note warning "Экспериментальная функциональность" %}

Функциональность находится в разработке и по умолчанию выключена флагом [`enable_checksum_table`](../reference/configuration/feature_flags.md).

{% include [experimental-feature-warning-body](experimental-feature-warning-body.md) %}

{% endnote %}
{% endif %}
