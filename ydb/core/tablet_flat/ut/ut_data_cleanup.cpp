#include "flat_executor_ut_common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable {

using namespace NTabletFlatExecutor;

enum : ui32  {
    KeyColumnId = 1,
    ValueColumnId = 2,
};

class TTxInitSchema : public ITransaction {
public:
    TTxInitSchema(const TVector<ui32>& tableIds)
        : TableIds(tableIds)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TCompactionPolicy policy;
        policy.MinBTreeIndexNodeSize = 128;

        for (const auto& tableId : TableIds) {
            if (txc.DB.GetScheme().GetTableInfo(tableId)) {
                continue;
            }

            txc.DB.Alter()
                .AddTable("test" + ToString(tableId), tableId)
                .AddColumn(tableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
                .AddColumn(tableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
                .AddColumnToKey(tableId, KeyColumnId)
                .SetCompactionPolicy(tableId, policy);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    const TVector<ui32> TableIds;
};

class TTxWriteRow : public ITransaction {
public:
    TTxWriteRow(ui32 tableId, i64 key, TString value)
        : TableId(tableId)
        , Key(key)
        , Value(std::move(value))
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = NScheme::TInt64::TInstance(Key);

        const auto val = NScheme::TString::TInstance(Value);
        NTable::TUpdateOp ops{ ValueColumnId, NTable::ECellOp::Set, val };

        txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, { ops });
        
        return true;
    }

    void Complete(const TActorContext&ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    i64 Key;
    TString Value;
};

class TTxDeleteRow : public ITransaction {
public:
    TTxDeleteRow(ui32 tableId, i64 key)
        : TableId(tableId)
        , Key(key)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = NScheme::TInt64::TInstance(Key);

        txc.DB.Update(TableId, NTable::ERowOp::Erase, { key }, {});
        
        return true;
    }

    void Complete(const TActorContext&ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    i64 Key;
};

class TTxFullScan : public ITransaction {
public:
    TTxFullScan(ui32 tableId, int& readRows)
        : TableId(tableId)
        , ReadRows(readRows)
    {
        ReadRows = 0;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &) override
    {
        TVector<NTable::TTag> tags{ { KeyColumnId, ValueColumnId } };

        auto iter = txc.DB.IterateRange(TableId, { }, tags);

        ReadRows = 0;
        while (iter->Next(ENext::Data) == EReady::Data) {
            ReadRows++;
        }

        if (iter->Last() != EReady::Page) {
            return true;
        }

        return false;
    }

    void Complete(const TActorContext &ctx) override
    {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    int& ReadRows;
};

bool ContainsInBlobStorage(TMyEnvBase& env, const TVector<TString>& values) {
    for (auto group : xrange(env.StorageGroupCount)) {
        env.SendEvToBSProxy(group, new NFake::TEvBlobStorageContainsRequest(values));
        auto ev = env.GrabEdgeEvent<NFake::TEvBlobStorageContainsResponse>();
        if (AnyOf(ev->Get()->Contains, [](bool v) { return v; }) ) {
            return true;
        }
    }
    return false;
}

Y_UNIT_TEST_SUITE(DataCleanup) {
    Y_UNIT_TEST(CleanupDataNoTables) {
        TMyEnvBase env;
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Clean));

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>();
    }

    Y_UNIT_TEST(CleanupData) {
        TMyEnvBase env;
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Clean));
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, "Some_value") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>();

        UNIT_ASSERT(!ContainsInBlobStorage(env, {"Some_value"}));
    }

    Y_UNIT_TEST(CleanupDataMultipleTables) {
        TMyEnvBase env;
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Clean));
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101, 102, 103 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, "Some_value") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(102, 43, "Some_other_value") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(103, 44, "Some_another_value") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(102, 43) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(103, 44) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(102, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(103, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>();

        UNIT_ASSERT(!ContainsInBlobStorage(env, {"Some_value", "Some_other_value", "Some_another_value"}));
    }

    Y_UNIT_TEST(CleanupDataWithFollowers) {
        TMyEnvBase env;
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Clean));

        env.FireDummyFollower(1);

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 41, "Some_value_1") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, "Some_value_2") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 41) });

        env.FireDummyFollower(2);
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, "Some_value_3") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>();

        UNIT_ASSERT(!ContainsInBlobStorage(env, {"Some_value_1", "Some_value_2", "Some_value_3"}));
    }

    Y_UNIT_TEST(CleanupDataMultipleTimes) {
        TMyEnvBase env;
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Clean));
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, "Some_value_1") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>(2);

        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, "Some_value_2") });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendAsync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.SendAsync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>(2);

        UNIT_ASSERT(!ContainsInBlobStorage(env, {"Some_value_1", "Some_value_2"}));
    }

    Y_UNIT_TEST(CleanupDataEmptyTable) {
        TMyEnvBase env;
        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Clean));
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->CleanupData();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.WaitFor<NFake::TEvDataCleaned>();
    }
}
} // namespace NKikimr::NTable
