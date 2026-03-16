from umongo.fields import ListField, EmbeddedField
from umongo.document import DocumentImplementation
from umongo.embedded_document import EmbeddedDocumentImplementation


def map_entry(entry, fields):
    """
    Retrieve the entry from the given fields and replace it if it should
    have a different name within the database.

    :param entry: is one of the followings:
        - invalid field name
        - command (i.g. $eq)
        - valid field with no attribute name
        - valid field with an attribute name to use instead
    """
    field = fields.get(entry)
    if isinstance(field, ListField) and isinstance(field.inner, EmbeddedField):
        fields = field.inner.embedded_document_cls.schema.fields
    elif isinstance(field, EmbeddedField):
        fields = field.embedded_document_cls.schema.fields
    return getattr(field, 'attribute', None) or entry, fields


def map_entry_with_dots(entry, fields):
    """
    Consider the given entry can be a '.' separated combination of single entries.
    """
    mapped = []
    for sub_entry in entry.split('.'):
        mapped_sub_entry, fields = map_entry(sub_entry, fields)
        mapped.append(mapped_sub_entry)
    return '.'.join(mapped), fields


def map_query(query, fields):
    """
    Retrieve given fields whithin the query and replace there name with
    the one they should have within the database.
    """
    if isinstance(query, dict):
        mapped_query = {}
        for entry, entry_query in query.items():
            mapped_entry, entry_fields = map_entry_with_dots(entry, fields)
            mapped_query[mapped_entry] = map_query(entry_query, entry_fields)
        return mapped_query
    if isinstance(query, (list, tuple)):
        return [map_query(x, fields) for x in query]
    # Passing a Document only makes sense in a Reference, let's query on ObjectId
    if isinstance(query, DocumentImplementation):
        return query.pk
    if isinstance(query, EmbeddedDocumentImplementation):
        return query.to_mongo()
    return query
