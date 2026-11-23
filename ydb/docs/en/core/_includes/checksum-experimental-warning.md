{% if feature_flags.enable_checksum_table == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default by the [`enable_checksum_table`](../reference/configuration/feature_flags.md) flag. Its behavior may change without backward compatibility, and the feature may never become generally available. Enabling the flag may cause errors, cluster instability, or make it impossible to change the YDB version. Do not enable this flag in production clusters. [Learn more](../reference/configuration/feature_flags.md#experimental).

{% endnote %}
{% endif %}
