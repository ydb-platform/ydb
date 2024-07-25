{% if backend_name == "YDB" and oss %}

{% note alert %}

{% include [not_allow_for_olap_text](not_allow_for_olap_text.md) %}

{% endnote %}

{% endif %}
