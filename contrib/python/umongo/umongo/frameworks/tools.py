from ..query_mapper import map_query


def cook_find_filter(doc_cls, filter):
    """
    Add the `_cls` field if needed and replace the fields' name by the one
    they have in database.
    """
    filter = map_query(filter, doc_cls.schema.fields)
    if doc_cls.opts.is_child:
        filter = filter or {}
        # Filter should be either a dict or an id
        if not isinstance(filter, dict):
            filter = {'_id': filter}
        # Current document shares the collection with a parent,
        # we must use the _cls field to discriminate
        if doc_cls.opts.offspring:
            # Current document has itself offspring, we also have
            # to search through them
            filter['_cls'] = {
                '$in': [o.__name__ for o in doc_cls.opts.offspring] + [doc_cls.__name__]}
        else:
            filter['_cls'] = doc_cls.__name__
    return filter


def remove_cls_field_from_embedded_docs(dict_in, embedded_docs):
    """Recursively remove _cls field from nested embedded documents

    This is meant to be used in umongo 2 to 3 migration. The embedded_docs list
    should be the list of concrete embedded documents that are not subclasses
    of a concrete document.

    :param dict dict_in: Input document content (dump)
    :param list embedded_docs: List of embedded documents for which to remove _cls
    """
    if isinstance(dict_in, dict):
        return {
            k: remove_cls_field_from_embedded_docs(v, embedded_docs)
            for k, v in dict_in.items()
            if k != "_cls" or v not in embedded_docs
        }
    if isinstance(dict_in, list):
        return [
            remove_cls_field_from_embedded_docs(item, embedded_docs)
            for item in dict_in
        ]
    return dict_in
