{% note warning %}

Currently, this command does not restore [secrets](../../../../concepts/datamodel/secrets.md) (this data is not included in the backup). You must [create](../../../../yql/reference/syntax/create-secret.md) secrets manually during restore before restoring objects that use them, such as [data transfers](../../../../concepts/transfer.md).

{% endnote %}
