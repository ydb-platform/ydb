* The index is always [local](../../../../concepts/glossary.md#local-index) (`LOCAL`); there is no [global](../../../../concepts/glossary.md#secondary-index) variant.
* Queries do not use the `VIEW <index>` syntax (unlike, for example, [fulltext indexes](../../../../dev/fulltext-indexes.md)).
* The filter is applied on read only to data fragments where an index block was already stored during a write or [portion merge](../../../../concepts/glossary.md#compaction), other fragments are not skipped by this index until merge.
