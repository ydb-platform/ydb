{% if feature_column_container_type != true %}

{{ backend_name }} tables don't support container types, so the FLATTEN BY function can only be applied to table-type variables created within a YQL query.

{% endif %}
