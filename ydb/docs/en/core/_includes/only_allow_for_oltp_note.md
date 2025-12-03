{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [only_allow_for_oltp_text](only_allow_for_oltp_text.md) %}

{% endnote %}

{% endif %}