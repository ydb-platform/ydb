{% if tech or feature_mapreduce %}

If you depend on some features of a certain regular expression engine, it's better to internally implement UDF with a given library inside. Use Pcre as the current most-recommended option for simple matching (that can be potentially changed to something else).

{% endif %}
