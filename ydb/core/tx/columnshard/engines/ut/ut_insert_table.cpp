#include "helper.h"
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/insert_table/insert_table.h>

#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>

namespace NKikimr {

using namespace NOlap;
using namespace NKikimr::NOlap::NEngines::NTest;

namespace {

class TTestInsertTableDB: public IDbWrapper {
public:
    void Insert(const TInsertedData&) override {}
    void Commit(const TInsertedData&) override {}
    void Abort(const TInsertedData&) override {}
    void EraseInserted(const TInsertedData&) override {}
    void EraseCommitted(const TInsertedData&) override {}
    void EraseAborted(const TInsertedData&) override {}

    bool Load(TInsertTableAccessor&,
        const TInstant&) override {
        return true;
    }

    void WriteColumn(const TPortionInfo&, const TColumnRecord&) override {}
    void EraseColumn(const TPortionInfo&, const TColumnRecord&) override {}
    bool LoadColumns(const std::function<void(const TPortionInfo&, const TColumnChunkLoadContext&)>&) override { return true; }

    virtual void WriteIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {}
    virtual void EraseIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {}
    virtual bool LoadIndexes(const std::function<void(const ui64 /*pathId*/, const ui64 /*portionId*/, const TIndexChunkLoadContext&)>& /*callback*/) override { return true; }

    void WriteCounter(ui32, ui64) override {}
    bool LoadCounters(const std::function<void(ui32 id, ui64 value)>&) override { return true; }
};

}

Y_UNIT_TEST_SUITE(TColumnEngineTestInsertTable) {
    Y_UNIT_TEST(TestInsertCommit) {
        ui64 writeId = 0;
        ui64 tableId = 0;
        TString dedupId = "0";
        TUnifiedBlobId blobId1(2222, 1, 1, 100, 1);

        TTestInsertTableDB dbTable;
        TInsertTable insertTable;
        ui64 indexSnapshot = 0;

        // insert, not commited
        bool ok = insertTable.Insert(dbTable, TInsertedData(writeId, tableId, dedupId, blobId1, TLocalHelper::GetMetaProto(), indexSnapshot, {}));
        UNIT_ASSERT(ok);

        // insert the same blobId1 again
        ok = insertTable.Insert(dbTable, TInsertedData(writeId, tableId, dedupId, blobId1, TLocalHelper::GetMetaProto(), indexSnapshot, {}));
        UNIT_ASSERT(!ok);

        // insert different blodId with the same writeId and dedupId
        TUnifiedBlobId blobId2(2222, 1, 2, 100, 1);
        ok = insertTable.Insert(dbTable, TInsertedData(writeId, tableId, dedupId, blobId2, TLocalHelper::GetMetaProto(), indexSnapshot, {}));
        UNIT_ASSERT(!ok);

        // read nothing
        auto blobs = insertTable.Read(tableId, TSnapshot::Zero(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);
        blobs = insertTable.Read(tableId+1, TSnapshot::Zero(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);

        // commit
        ui64 planStep = 100;
        ui64 txId = 42;
        insertTable.Commit(dbTable, planStep, txId, {TWriteId{writeId}}, [](ui64){ return true; });

        UNIT_ASSERT_EQUAL(insertTable.GetPathPriorities().size(), 1);
        UNIT_ASSERT_EQUAL(insertTable.GetPathPriorities().begin()->second.size(), 1);
        UNIT_ASSERT_EQUAL((*insertTable.GetPathPriorities().begin()->second.begin())->GetCommitted().size(), 1);

        // read old snapshot
        blobs = insertTable.Read(tableId, TSnapshot::Zero(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);
        blobs = insertTable.Read(tableId+1, TSnapshot::Zero(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);

        // read new snapshot
        blobs = insertTable.Read(tableId, TSnapshot(planStep, txId), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 1);
        blobs = insertTable.Read(tableId+1, TSnapshot::Zero(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);
    }
}

}
