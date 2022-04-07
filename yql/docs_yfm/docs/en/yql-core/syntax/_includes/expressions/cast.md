## CAST {#cast}

Tries to cast the value to the specified type. The attempt may fail and return `NULL`. When used with numbers, it may lose precision or most significant bits.
{% if feature_column_container_type %}
For lists and dictionaries, it can either delete or replace with `NULL` the elements whose conversion failed.
For structures and tuples, it deletes elements that are omitted in the target type.
For more information about casting rules, see [here](../../../types/cast.md).
{% endif %}

{% include [decimal_args](../../../_includes/decimal_args.md) %}

**Examples**

{% include [cast_examples](../../../_includes/cast_examples.md) %}

