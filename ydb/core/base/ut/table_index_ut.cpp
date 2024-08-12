#include "table_index.h"
#include "table_vector_index.h"

#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr::NTableIndex;

const TTableColumns Table{{"PK1", "PK2", "DATA1", "DATA2", "DATA3"}, {"PK2", "PK1"}};

Y_UNIT_TEST_SUITE (TableIndex) {
    Y_UNIT_TEST (CompatibleSecondaryIndex) {
        TString explain;
        auto type = NKikimrSchemeOp::EIndexType::EIndexTypeGlobal;

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"DATA1"}, {}}, explain));
        UNIT_ASSERT(explain.empty());

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"DATA1", "DATA2"}, {}}, explain));
        UNIT_ASSERT(explain.empty());

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"PK1", "PK2"}, {}}, explain));
        UNIT_ASSERT(explain.empty());

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA3"}}, explain));
        UNIT_ASSERT(explain.empty());

        {
            const TTableColumns Table2{{"PK", "DATA", NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {"PK"}};

            UNIT_ASSERT(IsCompatibleIndex(type, Table2, {{NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {}}, explain));
            UNIT_ASSERT(explain.empty());

            UNIT_ASSERT(IsCompatibleIndex(type, Table2, {{"DATA"}, {NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}}, explain));
            UNIT_ASSERT(explain.empty());
        }
        {
            const TTableColumns Table3{{"PK", "DATA", NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}};

            UNIT_ASSERT(IsCompatibleIndex(type, Table3, {{"DATA"}, {}}, explain));
            UNIT_ASSERT(explain.empty());
        }
    }

    Y_UNIT_TEST (NotCompatibleSecondaryIndex) {
        TString explain;
        auto type = NKikimrSchemeOp::EIndexType::EIndexTypeGlobal;

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"NOT EXIST"}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index key columns should be in table columns, index key column NOT EXIST is missed");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index data columns should be in table columns, index data column NOT EXIST is missed");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"NOT EXIST"}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index key columns should be in table columns, index key column NOT EXIST is missed");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1", "DATA1"}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index key columns should be unique, for example DATA1");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"PK2"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "the same column can't be used as key and data column for one index, for example PK2");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA1"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "the same column can't be used as key and data column for one index, for example DATA1");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA3", "DATA3"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index data columns should be unique, for example DATA3");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "should be at least single index key column");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"PK2"}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "index keys are prefix of table keys");
    }

    Y_UNIT_TEST (CompatibleVectorIndex) {
        TString explain;
        auto type = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"DATA1"}, {}}, explain));
        UNIT_ASSERT(explain.empty());

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA3"}}, explain));
        UNIT_ASSERT(explain.empty());

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"PK1"}, {}}, explain));
        UNIT_ASSERT(explain.empty());

        UNIT_ASSERT(IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA1"}}, explain));
        UNIT_ASSERT(explain.empty());
    }

    Y_UNIT_TEST (NotCompatibleVectorIndex) {
        TString explain;
        auto type = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"NOT EXIST"}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index key columns should be in table columns, index key column NOT EXIST is missed");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index data columns should be in table columns, index data column NOT EXIST is missed");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"NOT EXIST"}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index key columns should be in table columns, index key column NOT EXIST is missed");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1", "DATA1"}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "all index key columns should be unique, for example DATA1");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1", "DATA2"}, {}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "only single key column is supported for vector index");

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"PK2"}}, explain));
        UNIT_ASSERT_STRINGS_EQUAL(explain, "the same column can't be used as key and data column for one index, for example PK2");

        {
            const TTableColumns Table2{{"PK", "DATA", NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {"PK"}};

            UNIT_ASSERT(!IsCompatibleIndex(type, Table2, {{NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {}}, explain));
            UNIT_ASSERT_STRINGS_EQUAL(explain, TStringBuilder() << "index key column shouldn't have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);

            UNIT_ASSERT(!IsCompatibleIndex(type, Table2, {{"DATA"}, {NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}}, explain));
            UNIT_ASSERT_STRINGS_EQUAL(explain, TStringBuilder() << "index data column shouldn't have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);
        }
        {
            const TTableColumns Table3{{"PK", "DATA", NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}};

            UNIT_ASSERT(!IsCompatibleIndex(type, Table3, {{"DATA"}, {}}, explain));
            UNIT_ASSERT_STRINGS_EQUAL(explain, TStringBuilder() << "table key column shouldn't have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);
        }
    }
}

}
