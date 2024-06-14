
{% include [x](_includes/basic/intro.md) %}

{% include [x](_includes/basic/coalesce.md) %}

{% include [x](_includes/basic/length.md) %}

{% include [x](_includes/basic/substring.md) %}

{% include [x](_includes/basic/find.md) %}

{% include [x](_includes/basic/starts_ends_with.md) %}

{% include [x](_includes/basic/if.md) %}

{% include [x](_includes/basic/nanvl.md) %}

{% include [x](_includes/basic/random.md) %}

{% include [x](_includes/basic/udf.md) %}

{% include [x](_includes/basic/current_utc.md) %}

{% include [x](_includes/basic/current_tz.md) %}

{% include [x](_includes/basic/max_min.md) %}

{% include [x](_includes/basic/as_container.md) %}

{% include [x](_includes/basic/container_literal.md) %}

{% include [x](_includes/basic/variant.md) %}

{% include [x](_includes/basic/enum.md) %}

{% include [x](_includes/basic/as_tagged.md) %}

{% if feature_bulk_tables %}

  {% include [x](_includes/basic/table_path_name_recindex.md) %}

{% endif %}

{% include [x](_includes/basic/table_row.md) %}

{% if feature_mapreduce %}

  {% include [x](_includes/basic/files.md) %}

  {% include [x](_includes/basic/weakfield.md) %}

{% endif %}

{% include [x](_includes/basic/ensure.md) %}

{% include [x](_includes/basic/assume_strict.md) %}

{% include [x](_includes/basic/likely.md) %}

{% if feature_codegen %}

  {% include [x](_includes/basic/evaluate_expr_atom.md) %}

{% endif %}

{% include [x](_includes/basic/data-type-literals.md) %}

{% if feature_webui %}

  {% include [x](_includes/basic/metadata.md) %}

{% endif %}

{% include [x](_includes/basic/to_from_bytes.md) %}

{% include [x](_includes/basic/byteat.md) %}

{% include [x](_includes/basic/bitops.md) %}

{% include [x](_includes/basic/abs.md) %}

{% include [x](_includes/basic/optional_ops.md) %}

{% include [x](_includes/basic/callable.md) %}

{% include [x](_includes/basic/pickle.md) %}

{% include [x](_includes/basic/staticmap.md) %}

{% include [x](_includes/basic/staticzip.md) %}

{% include [x](_includes/basic/staticfold.md) %}

{% include [x](_includes/basic/aggr_factory.md) %}

{% if tech %}

  {% include [x](_includes/basic/s_expressions.md) %}

{% endif %}
