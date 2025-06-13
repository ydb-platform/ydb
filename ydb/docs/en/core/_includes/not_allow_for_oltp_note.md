{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLTP_not_allow_text](not_allow_for_oltp_text.md) %}

{% endnote %}

{% endif %}
