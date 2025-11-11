{% if oss == true and backend_name == "YDB" %}

{% note alert %}

{% include [not_allow_for_olap_text](not_allow_for_oltp_text.md) %}

{% endnote %}

{% endif %}
