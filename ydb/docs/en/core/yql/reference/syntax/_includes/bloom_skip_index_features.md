* The index is always [local](../../../../concepts/glossary.md#local-index) (`LOCAL`); there is no [global](../../../../concepts/glossary.md#secondary-index) variant.
* Queries do not use the `VIEW <index>` syntax (unlike, for example, [fulltext indexes](../../../../dev/fulltext-indexes.md)).
* The filter is used on read only for data where an index block was already stored on write or rebuild. Other fragments are not skipped by this index until rebuild.
