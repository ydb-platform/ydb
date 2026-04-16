#include "flat_executor_ut_common.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>

namespace NKikimr::NTable {

using namespace NTabletFlatExecutor;

enum : ui32  {
    KeyColumnId = 1,
    ValueColumnId = 2,
    ValueFamily2ColumnId = 3,
    ValueFamily3ColumnId = 4,
};

enum : ui32  {
    FamilyId2Channel = 2,
    FamilyId3Channel = 3,
    RoomId2 = 102,
    RoomId3 = 103,
    FamilyId2 = 202,
    FamilyId3 = 205,
};

class TTxInitSchema : public ITransaction {
public:
    TTxInitSchema(const TVector<ui32>& tableIds, bool addColumnsInFamily = false, bool allowLogBatching = false)
        : TableIds(tableIds)
        , AddColumnsInFamily(addColumnsInFamily)
        , AllowLogBatching(allowLogBatching)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TCompactionPolicy policy;
        policy.MinBTreeIndexNodeSize = 128;

        for (const auto& tableId : TableIds) {
            if (txc.DB.GetScheme().GetTableInfo(tableId)) {
                continue;
            }

            auto alter = txc.DB.Alter()
                .AddTable("test" + ToString(tableId), tableId)
                .AddColumn(tableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false, false)
                .AddColumn(tableId, "value", ValueColumnId, NScheme::TString::TypeId, false, false)
                .AddColumnToKey(tableId, KeyColumnId)
                .SetCompactionPolicy(tableId, policy)
                .SetExecutorAllowLogBatching(AllowLogBatching);

            if (AddColumnsInFamily) {
                txc.DB.Alter()
                    .SetRoom(tableId, RoomId2, FamilyId2Channel, {FamilyId2Channel}, FamilyId2Channel)
                    .AddFamily(tableId, FamilyId2, RoomId2)
                    .AddColumn(tableId, "value_family_2", ValueFamily2ColumnId, NScheme::TString::TypeId, false, false)
                    .AddColumnToFamily(tableId, ValueFamily2ColumnId, FamilyId2);

                txc.DB.Alter()
                    .SetRoom(tableId, RoomId3, FamilyId3Channel, {FamilyId3Channel}, FamilyId3Channel)
                    .AddFamily(tableId, FamilyId3, RoomId3)
                    .AddColumn(tableId, "value_family_3", ValueFamily3ColumnId, NScheme::TString::TypeId, false, false)
                    .AddColumnToFamily(tableId, ValueFamily3ColumnId, FamilyId3);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    const TVector<ui32> TableIds;
    const bool AddColumnsInFamily;
    const bool AllowLogBatching;
};

class TTxWriteRow : public ITransaction {
public:
    TTxWriteRow(ui32 tableId, i64 key, TString value, ui32 valueColumnId = ValueColumnId)
        : TableId(tableId)
        , Key(key)
        , Value(std::move(value))
        , ValueColumnId_(valueColumnId)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = NScheme::TInt64::TInstance(Key);

        const auto val = NScheme::TString::TInstance(Value);
        NTable::TUpdateOp ops{ ValueColumnId_, NTable::ECellOp::Set, val };

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
    ui32 ValueColumnId_;
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

int BlobStorageValueCount(TMyEnvBase& env, const TString& value, ui32 channel) {
    int count = 0;
    env.SendEvToBSProxy(channel, new NFake::TEvBlobStorageContainsRequest(value));
    auto ev = env.GrabEdgeEvent<NFake::TEvBlobStorageContainsResponse>();
    for (const auto& blobInfo : ev->Get()->Contains) {
        UNIT_ASSERT(blobInfo.BlobId.Channel() == channel);
        if (!blobInfo.DoNotKeep) {
            ++count;
        }
    }
    return count;
}

int BlobStorageValueCountInAllGroups(TMyEnvBase& env, const TString& value) {
    int count = 0;
    for (auto group : xrange(env.StorageGroupCount)) {
        count += BlobStorageValueCount(env, value, group);
    }
    return count;
}

Y_UNIT_TEST_SUITE(Vacuum) {
    ui32 TestTabletFlags = ui32(NFake::TDummy::EFlg::Comp) | ui32(NFake::TDummy::EFlg::Vac);

    Y_UNIT_TEST(StartVacuumNoTables) {
        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        auto ev = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->VacuumGeneration, 234);
    }

    Y_UNIT_TEST(StartVacuumNoTablesWithRestart) {
        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev1 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->VacuumGeneration, 234);

        env.RestartTablet(TestTabletFlags);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(235);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } }, true);
        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 235);
    }

    Y_UNIT_TEST(StartVacuumLog) {
        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }, true) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, "Some_value") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, "Some_other_value", ValueFamily2ColumnId) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        // short string should be present uncompressed in log and sst
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, "Some_value", 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, "Some_other_value", 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, "Some_other_value", 2), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->VacuumGeneration, 234);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, "Some_value"), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, "Some_other_value"), 0);
    }

    Y_UNIT_TEST(StartVacuum) {
        TString value42(size_t(100 * 1024), 'a');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->VacuumGeneration, 234);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
    }

    Y_UNIT_TEST(StartVacuumMultipleFamilies) {
        TString value42(size_t(100 * 1024), 'a');
        TString value43(size_t(100 * 1024), 'b');
        TString value44(size_t(100 * 1024), 'c');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }, true) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, value43, ValueFamily2ColumnId) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 44, value44, ValueFamily3ColumnId) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value44, 3), 1);

        // delete row with value in family 3 and cleanup

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 44) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 2);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev1 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->VacuumGeneration, 234);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value44), 0);

        // restart and delete other rows with deffered GC in family 2

        env.RestartTablet(TestTabletFlags);

        readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) }, true);
        UNIT_ASSERT_EQUAL(readRows, 2);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value44), 0);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });

        // defer GC after compaction in channel 2
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> gcEvents(*env, [](const auto& ev) {
            return ev->Get()->Channel == 2;
        });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(235);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env->WaitFor("gc events", [&gcEvents]{ return gcEvents.size() >= 1; });

        // should not be cleaned without GC
        UNIT_ASSERT(!env.GrabEdgeEvent<NFake::TEvDataCleaned>(TDuration::Seconds(10)));

        gcEvents.Stop().Unblock();
        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 235);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value44), 0);
    }

    Y_UNIT_TEST(StartVacuumMultipleTables) {
        TString value42(size_t(100 * 1024), 'a');
        TString value43(size_t(100 * 1024), 'b');
        TString value44(size_t(100 * 1024), 'c');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101, 102, 103 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(102, 43, value43) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(103, 44, value44) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();
        env.SendSync(new NFake::TEvCompact(102));
        env.WaitFor<NFake::TEvCompacted>();
        env.SendSync(new NFake::TEvCompact(103));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value44, 1), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });
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
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->VacuumGeneration, 234);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value44), 0);
    }

    Y_UNIT_TEST(StartVacuumWithFollowers) {
        TString value41(size_t(100 * 1024), 'a');
        TString value42(size_t(100 * 1024), 'b');
        TString value43(size_t(100 * 1024), 'c');

        TMyEnvBase env;
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
        env->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_TRACE);
        env.FireDummyTablet(TestTabletFlags);

        env.FireDummyFollower(1);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 41, value41) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });

        env.FireDummyFollower(2);
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, value43) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value41, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 1), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 41) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->VacuumGeneration, 234);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value41), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);
    }

    Y_UNIT_TEST(StartVacuumMultipleTimes) {
        TString value42(size_t(100 * 1024), 'b');
        TString value43(size_t(90 * 1024), 'f');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            executor->StartVacuum(235);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev1 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->VacuumGeneration, 235); // only last genration should be present

        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, value43) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 1), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendAsync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(236);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env.SendAsync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(237);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 237); // only last genration should be present

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);
    }

    Y_UNIT_TEST(StartVacuumEmptyTable) {
        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        auto ev = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->VacuumGeneration, 234);
    }

    Y_UNIT_TEST(StartVacuumWithRestarts) {
        TString value42(size_t(100 * 1024), 'a');
        TString value43(size_t(100 * 1024), 'b');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, value43) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 1), 1);

        env.RestartTablet(TestTabletFlags);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) }, true);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev1 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->VacuumGeneration, 234);

        env.RestartTablet(TestTabletFlags);

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) }, true);
        UNIT_ASSERT_EQUAL(readRows, 1);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        // defer GC after compaction in channel 1
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> gcEvents(*env, [](const auto& ev) {
            return ev->Get()->Channel == 1;
        });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(235);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env->WaitFor("gc events", [&gcEvents]{ return gcEvents.size() >= 1; });

        // should not be cleaned without GC
        UNIT_ASSERT(!env.GrabEdgeEvent<NFake::TEvDataCleaned>(TDuration::Seconds(10)));

        gcEvents.Stop().Unblock();
        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 235);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);
    }

    Y_UNIT_TEST(StartVacuumRetryWithNotGreaterGenerations) {
        TString value42(size_t(100 * 1024), 'a');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) });
        UNIT_ASSERT_VALUES_EQUAL(readRows, 0);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev1 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->VacuumGeneration, 234);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(115);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 234);

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(234);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        auto ev3 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev3->Get()->VacuumGeneration, 234);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
    }

    Y_UNIT_TEST(StartVacuumWithTabletGCErrors) {
        TString value42(size_t(100 * 1024), 'a');
        TString value43(size_t(100 * 1024), 'b');

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }, true) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 43, value43, ValueFamily2ColumnId) });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 2), 1);

        env.RestartTablet(TestTabletFlags);

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) }, true);
        UNIT_ASSERT_EQUAL(readRows, 2);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value43, 2), 1);

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });
        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 43) });

        // channel 2 contains only compacted table data
        TBlockEvents<TEvBlobStorage::TEvCollectGarbageResult> gcResults(*env, [](const auto& ev) {
            return ev->Get()->Channel == 2;
        });

        env.SendSync(new NFake::TEvCompact(101));
        env.WaitFor<NFake::TEvCompacted>();

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(235);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        env->WaitFor("gc results", [&gcResults]{ return gcResults.size() >= 1; });

        // should not be cleaned without GC
        UNIT_ASSERT(!env.GrabEdgeEvent<NFake::TEvDataCleaned>(TDuration::Seconds(10)));

        // convert all caught results to errors that should be retried
        TWaitForFirstEvent<TEvBlobStorage::TEvCollectGarbageResult> retriedGcResults(*env, [](const auto& ev) {
            return ev->Get()->Channel == 2 && ev->Get()->Status == NKikimrProto::OK;
        });
        for (auto& gcResult : gcResults) {
            gcResult->Get()->Status = NKikimrProto::ERROR;
        }
        gcResults.Stop().Unblock();
        retriedGcResults.Wait();
        retriedGcResults.Stop();

        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 235);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value43), 0);
    }

    Y_UNIT_TEST(StartVacuumWithSysTabletGCErrors) {
        TString value42 = "Some_value";

        TMyEnvBase env;
        env.Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env.FireDummyTablet(TestTabletFlags);
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }, false, true) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(101, 42, value42) });

        int readRows = 0;
        env.SendSync(new NFake::TEvExecute{ new TTxFullScan(101, readRows) }, true);
        UNIT_ASSERT_EQUAL(readRows, 1);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCount(env, value42, 0), 1);

        // make all log GC responses failed
        auto obs = env->AddObserver<TEvBlobStorage::TEvCollectGarbageResult>([](auto& ev) {
            if (ev->Get()->Channel == 0) {
                ev->Get()->Status = NKikimrProto::ERROR;
            }
        });

        env.SendSync(new NFake::TEvExecute{ new TTxDeleteRow(101, 42) });

        env.SendSync(new NFake::TEvCall{ [](auto* executor, const auto& ctx) {
            executor->StartVacuum(235);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        // should not be cleaned without successful log GC
        UNIT_ASSERT(!env.GrabEdgeEvent<NFake::TEvDataCleaned>(TDuration::Seconds(10)));

        // make all subsequent log GC responses OK
        obs.Remove();

        auto ev2 = env.GrabEdgeEvent<NFake::TEvDataCleaned>();
        UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->VacuumGeneration, 235);

        UNIT_ASSERT_VALUES_EQUAL(BlobStorageValueCountInAllGroups(env, value42), 0);
    }
}
} // namespace NKikimr::NTable
