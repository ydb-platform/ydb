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
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"NOT EXIST"}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1", "DATA1"}, {}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"PK2"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA1"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"DATA3", "DATA3"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{}, {}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"PK2"}, {}}, explain));
        UNIT_ASSERT(!explain.empty());
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
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"NOT EXIST"}, {"NOT EXIST"}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1", "DATA1"}, {}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1", "DATA2"}, {}}, explain));
        UNIT_ASSERT(!explain.empty());

        UNIT_ASSERT(!IsCompatibleIndex(type, Table, {{"DATA1"}, {"PK2"}}, explain));
        UNIT_ASSERT(!explain.empty());

        {
            const TTableColumns Table2{{"PK", "DATA", NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {"PK"}};

            UNIT_ASSERT(!IsCompatibleIndex(type, Table2, {{NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {}}, explain));
            UNIT_ASSERT(!explain.empty());

            UNIT_ASSERT(!IsCompatibleIndex(type, Table2, {{"DATA"}, {NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}}, explain));
            UNIT_ASSERT(!explain.empty());
        }
        {
            const TTableColumns Table3{{"PK", "DATA", NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}, {NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn}};

            UNIT_ASSERT(!IsCompatibleIndex(type, Table3, {{"DATA"}, {}}, explain));
            UNIT_ASSERT(!explain.empty());
        }
    }
}

}
