#include "helper.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/insert_table/insert_table.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>

namespace NKikimr {

using namespace NOlap;
using namespace NKikimr::NOlap::NEngines::NTest;

namespace {

class TTestInsertTableDB : public IDbWrapper {
public:
    virtual const IBlobGroupSelector* GetDsGroupSelector() const override {
        return &Default<TFakeGroupSelector>();
    }

    virtual void WriteColumns(const NOlap::TPortionInfo& /*portion*/, const NKikimrTxColumnShard::TIndexPortionAccessor& /*proto*/) override {

    }
    void Insert(const TInsertedData&) override {
    }
    void Commit(const TCommittedData&) override {
    }
    void Abort(const TInsertedData&) override {
    }
    void EraseInserted(const TInsertedData&) override {
    }
    void EraseCommitted(const TCommittedData&) override {
    }
    void EraseAborted(const TInsertedData&) override {
    }

    virtual TConclusion<THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() override {
        THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>> result;
        return result;
    }

    bool Load(TInsertTableAccessor&, const TInstant&) override {
        return true;
    }

    virtual void WritePortion(const NOlap::TPortionInfo& /*portion*/) override {
    }
    virtual void ErasePortion(const NOlap::TPortionInfo& /*portion*/) override {
    }
    virtual bool LoadPortions(const std::optional<TInternalPathId> /*reqPathId*/,
        const std::function<void(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& /*callback*/) override {
        return true;
    }

    void WriteColumn(const TPortionInfo&, const TColumnRecord&, const ui32 /*firstPKColumnId*/) override {
    }
    void EraseColumn(const TPortionInfo&, const TColumnRecord&) override {
    }
    bool LoadColumns(const std::optional<TInternalPathId> /*reqPathId*/, const std::function<void(TColumnChunkLoadContextV2&&)>&) override {
        return true;
    }

    virtual void WriteIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {
    }
    virtual void EraseIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {
    }
    virtual bool LoadIndexes(const std::optional<TInternalPathId> /*reqPathId*/,
        const std::function<void(const TInternalPathId /*pathId*/, const ui64 /*portionId*/, TIndexChunkLoadContext&&)>& /*callback*/) override {
        return true;
    }

    void WriteCounter(ui32, ui64) override {
    }
    bool LoadCounters(const std::function<void(ui32 id, ui64 value)>&) override {
        return true;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnEngineTestInsertTable) {
    Y_UNIT_TEST(TestInsertCommit) {
        TInsertWriteId writeId = (TInsertWriteId)0;
        const auto& tableId0 = TInternalPathId::FromRawValue(0);
        const auto& tableId1 = TInternalPathId::FromRawValue(1);
        TString dedupId = "0";
        TUnifiedBlobId blobId1(2222, 1, 1, 100, 2, 0, 1);

        TTestInsertTableDB dbTable;
        TInsertTable insertTable;
        ui64 indexSnapshot = 0;
        
        // insert, not commited
        auto userData1 = std::make_shared<TUserData>(tableId0, TBlobRange(blobId1), TLocalHelper::GetMetaProto(), indexSnapshot, std::nullopt);
        insertTable.RegisterPathInfo(tableId0);
        bool ok = insertTable.Insert(dbTable, TInsertedData(writeId, userData1));
        UNIT_ASSERT(ok);

        // read nothing
        auto blobs = insertTable.Read(tableId0, {}, TSnapshot::Zero(), TLocalHelper::GetMetaSchema(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);
        blobs = insertTable.Read(tableId1, {}, TSnapshot::Zero(), TLocalHelper::GetMetaSchema(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);

        // commit
        ui64 planStep = 100;
        ui64 txId = 42;
        insertTable.Commit(dbTable, planStep, txId, { writeId }, [](TInternalPathId) {
            return true;
        });
//        UNIT_ASSERT_EQUAL(insertTable.GetPathPriorities().size(), 1);
//        UNIT_ASSERT_EQUAL(insertTable.GetPathPriorities().begin()->second.size(), 1);
//        UNIT_ASSERT_EQUAL((*insertTable.GetPathPriorities().begin()->second.begin())->GetCommitted().size(), 1);

        // read old snapshot
        blobs = insertTable.Read(tableId0, {}, TSnapshot::Zero(), TLocalHelper::GetMetaSchema(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);
        blobs = insertTable.Read(tableId1, {}, TSnapshot::Zero(), TLocalHelper::GetMetaSchema(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);

        // read new snapshot
        blobs = insertTable.Read(tableId0, {}, TSnapshot(planStep, txId), TLocalHelper::GetMetaSchema(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 1);
        blobs = insertTable.Read(tableId1, {}, TSnapshot::Zero(), TLocalHelper::GetMetaSchema(), nullptr);
        UNIT_ASSERT_EQUAL(blobs.size(), 0);
    }
}

}   // namespace NKikimr
