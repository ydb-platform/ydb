## AsTagged, Untag {#as-tagged}

Wraps the value in the [Tagged data type](../../../types/special.md) with the specified tag, preserving the physical data type. `Untag`: The reverse operation.

Required arguments:

1. Value of any type.
2. Tag name.

Returns a copy of the value from the first argument with the specified tag in the data type.

Examples of use cases:

* Returns to the client's web interface the media files from BASE64-encoded strings{% if feature_webui %}. Tag support in the YQL Web UI [is described here](../../../interfaces/web_tagged.md){% endif %}.
{% if feature_mapreduce %}* Prevent passing of invalid values at the boundaries of UDF calls.{% endif %}
* Additional refinements at the level of returned columns types.

