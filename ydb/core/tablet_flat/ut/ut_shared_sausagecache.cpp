#include "flat_executor_ut_common.h"

#include <shared_cache_counters.h>
#include <util/system/sanitizers.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;

enum : ui32  {
    TableId = 101,
    Table2Id = 102,
    KeyColumnId = 1,
    ValueColumnId = 2,
};

using TRetriedCounters = TVector<ui32>;
using namespace NSharedCache;

void Increment(TRetriedCounters& retried, ui32 attempts) {
    if (attempts >= retried.size()) {
        retried.resize(attempts + 1);
    }
    retried.at(attempts)++;
}

struct TTxInitSchema : public ITransaction {
    
    TTxInitSchema(ui32 tableId = NSharedCache::TableId, bool tryKeepInMemory = false, std::optional<ui32> channel = {})
        : TableId(tableId)
        , TryKeepInMemory(tryKeepInMemory)
        , Channel(channel)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (txc.DB.GetScheme().GetTableInfo(TableId))
            return true;

        CompactionPolicy->MinBTreeIndexNodeSize = 128;

        txc.DB.Alter()
            .AddTable("test" + ToString(ui32(TableId)), TableId)
            .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
            .AddColumn(TableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
            .AddColumnToKey(TableId, KeyColumnId)
            .SetCompactionPolicy(TableId, *CompactionPolicy);

        if (TryKeepInMemory) {
            txc.DB.Alter()
                .SetFamilyCacheMode(TableId, NTable::TColumn::LeaderFamily, NTable::NPage::ECacheMode::TryKeepInMemory);
        }

        if (Channel) {
            txc.DB.Alter()
                .SetRoom(TableId, 1, *Channel, {*Channel}, *Channel)
                .AddFamily(TableId, NTable::TColumn::LeaderFamily, 1);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

    NLocalDb::TCompactionPolicyPtr CompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
    ui32 TableId;
    bool TryKeepInMemory;
    std::optional<ui32> Channel;
};

struct TTxWriteRow : public ITransaction {
    ui32 TableId;
    i64 Key;
    TString Value;

    explicit TTxWriteRow(ui32 tableId, i64 key, TString value)
        : TableId(tableId)
        , Key(key)
        , Value(std::move(value))
    { }

    explicit TTxWriteRow(i64 key, TString value)
        : TTxWriteRow(NSharedCache::TableId, key, std::move(value))
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
};

struct TTxReadRows : public ITransaction {
    ui32 TableId;
    TVector<i64> Keys;
    TRetriedCounters& Retried;
    ui32 Attempts = 0;
    bool& Completed;

    TTxReadRows(ui32 tableId, TVector<i64> keys, TRetriedCounters& retried, bool& completed)
        : TableId(tableId)
        , Keys(keys)
        , Retried(retried)
        , Completed(completed)
    {
        Completed = false;
    }

    TTxReadRows(ui32 tableId, i64 key, TRetriedCounters& retried)
        : TTxReadRows(tableId, TVector<i64>{key}, retried, Completed_)
    { }

    TTxReadRows(TVector<i64> keys, TRetriedCounters& retried, bool& completed)
        : TTxReadRows(NSharedCache::TableId, keys, retried, completed)
    { }

    TTxReadRows(TVector<i64> keys, TRetriedCounters& retried)
        : TTxReadRows(keys, retried, Completed_)
    { }

    TTxReadRows(i64 key, TRetriedCounters& retried)
        : TTxReadRows(TVector<i64>{key}, retried)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Increment(Retried, Attempts);
        Attempts++;

        bool ready = true;
        for (auto key : Keys) {
            TVector<TRawTypeValue> rawKey;
            rawKey.emplace_back(&key, sizeof(key), NScheme::TInt64::TypeId);

            TVector<NTable::TTag> tags;
            tags.push_back(KeyColumnId);
            tags.push_back(ValueColumnId);

            NTable::TRowState row;
            if (txc.DB.Select(TableId, rawKey, tags, row) == NTable::EReady::Page) {
                ready = false;
            }
        }

        return ready;
    }

    void Complete(const TActorContext& ctx) override {
        Completed = true;
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    bool Completed_;
};

struct TTxTryKeepInMemory : public ITransaction {
    ui32 TableId;
    bool TryKeepInMemory;

    TTxTryKeepInMemory(ui32 tableId, bool tryKeepInMemory)
        : TableId(tableId)
        , TryKeepInMemory(tryKeepInMemory)
    { }

    TTxTryKeepInMemory(bool tryKeepInMemory)
        : TTxTryKeepInMemory(NSharedCache::TableId, tryKeepInMemory)
    { }

    bool Execute(TTransactionContext &txc, const TActorContext &) override
    {
        using namespace NTable::NPage;

        txc.DB.Alter()
            .SetFamilyCacheMode(TableId, 0, TryKeepInMemory ? ECacheMode::TryKeepInMemory : ECacheMode::Regular);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
};

THolder<TSharedPageCacheCounters> GetSharedPageCounters(TMyEnvBase& env) {
    return MakeHolder<TSharedPageCacheCounters>(GetServiceCounters(env->GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
};

void LogCounters(THolder<TSharedPageCacheCounters>& counters) {
    Cerr << "Counters: Active:" << counters->ActiveBytes->Val() << "/" << counters->ActiveLimitBytes->Val() 
        << ", Passive:" << counters->PassiveBytes->Val() 
        << ", MemLimit:" << counters->MemLimitBytes->Val()
        << Endl;
}

void WaitEvent(TMyEnvBase& env, ui32 eventType, ui32 requiredCount = 1) {
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(eventType, requiredCount));
    env->DispatchEvents(options);
}

void RestartAndClearCache(TMyEnvBase& env, ui64 memoryLimit = Max<ui64>()) {
    env.SendSync(new TEvents::TEvPoison, false, true);
    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(memoryLimit));
    WaitEvent(env, NMemory::EvConsumerLimit);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
}

void SetupSharedCache(TMyEnvBase& env, ui64 limit = 8_MB, bool resetMemoryLimit = false) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    auto config = request->Record.MutableConfig()->MutableSharedCacheConfig();
    config->SetMemoryLimit(limit);
    
    env->Send(MakeSharedPageCacheId(), TActorId{}, request.Release());
    WaitEvent(env, NConsole::TEvConsole::EvConfigNotificationRequest);

    if (resetMemoryLimit) {
        env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(Max<ui64>()));
        WaitEvent(env, NMemory::EvConsumerLimit);
    }
}

// simulates other tablet shared cache usage
void WakeupSharedCache(TMyEnvBase& env) {
    env->Send(MakeSharedPageCacheId(), TActorId{}, new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCManual)));
    TWaitForFirstEvent<TKikimrEvents::TEvWakeup>(*env, [&](const auto& ev) {
        return ev->Get()->Tag == static_cast<ui64>(EWakeupTag::DoGCManual) 
            && env->FindActorName(ev->GetRecipientRewrite()) == "SAUSAGE_CACHE";
    }).Wait(TDuration::Seconds(5));
}

void DoReadRows(TMyEnvBase& env, TTxReadRows* read, bool retry = false) {
    env.SendSync(new NFake::TEvExecute{ read }, retry);

    WakeupSharedCache(env);
}

Y_UNIT_TEST_SUITE(TSharedPageCache) {

Y_UNIT_TEST(Limits) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    bool bTreeIndex = env->GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();
    ui32 passiveBytes = bTreeIndex ? 139 : 7772;

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env);

    // write 300 rows, each ~100KB (~30MB)
    for (i64 key = 0; key < 300; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 0; key < 100; ++key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->LoadInFlyBytes->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 0);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(100_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 100_MB);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(6_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(6_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 6_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 6_MB);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(3_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(3_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 3_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 3_MB);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 0_MB);
}

Y_UNIT_TEST(Limits_Config) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    bool bTreeIndex = env->GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();
    ui32 passiveBytes = bTreeIndex ? 139 : 7772;

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env);

    // write 300 rows, each ~100KB (~30MB)
    for (i64 key = 0; key < 300; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 0; key < 100; ++key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->LoadInFlyBytes->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 8_MB);

    SetupSharedCache(env, 100_MB);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 100_MB);

    SetupSharedCache(env, 2_MB);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(2_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 2_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 2_MB);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(1_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(1_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 1_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 1_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 2_MB);

    SetupSharedCache(env, 0_MB);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 0_MB);
}

Y_UNIT_TEST(S3FIFO) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 8_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 139);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 38}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 59, 2}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    DoReadRows(env, new TTxReadRows(0, retried), true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));

    retried = {};
    DoReadRows(env, new TTxReadRows(0, retried), true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    DoReadRows(env, new TTxReadRows(0, retried), true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 101, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 28, 4, 1}));
}

Y_UNIT_TEST(BigCache_BTreeIndex) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 20_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 118);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 118);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 118);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 118);
}

Y_UNIT_TEST(BigCache_FlatIndex) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(false);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 20_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 102);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 102);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 102);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 102);
}

Y_UNIT_TEST(MiddleCache_BTreeIndex) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 8_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 97);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 97);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(2_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 21);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 99);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(12_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 139);
}

Y_UNIT_TEST(MiddleCache_FlatIndex) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(false);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 8_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 81);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 2);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 81);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 2);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(2_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 19);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 83);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(12_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 121);
}

Y_UNIT_TEST(ZeroCache_BTreeIndex) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 0_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 100, 100, 100}));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 400);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 100, 100, 100}));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 802);
}

Y_UNIT_TEST(ZeroCache_FlatIndex) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(false);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 0_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 2);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried));
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100}));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 2);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 100);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100}));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 2);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 202);
}

Y_UNIT_TEST(TryKeepInMemoryMode_Basics) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    SetupSharedCache(env, 10_MB, true);

    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(TableId, true) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(TableId, key, std::move(value)) });
    }
    Cerr << "...compacting first table" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until first table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 119);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // make second table to try to preempt first table from cache
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(Table2Id, false) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(Table2Id, key, std::move(value)) });
    }
    Cerr << "...compacting second table" << Endl;
    env.SendSync(new NFake::TEvCompact(Table2Id));
    Cerr << "...waiting until second table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(Table2Id, key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 98, 13, 1}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 112);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // read from in-memory table, should be no more cache misses
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(TableId, key, retried), true);
    }

    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 112);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);

    RestartAndClearCache(env, 10_MB);

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(Table2Id, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 232);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // read from in-memory table, should be no more cache misses and all read should be from private cache (no more CacheHit*)
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(TableId, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 232);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);
}

Y_UNIT_TEST(TryKeepInMemoryMode_Enabling) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    SetupSharedCache(env, 10_MB, true);

    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(TableId, false) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(TableId, key, std::move(value)) });
    }
    Cerr << "...compacting first table" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until first table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 118);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));

    // Enable in-memory for first table
    env.SendSync(new NFake::TEvExecute{ new TTxTryKeepInMemory(TableId, true) });
    
    // make second table to try to preempt first table from cache
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(Table2Id, false) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(Table2Id, key, std::move(value)) });
    }
    Cerr << "...compacting second table" << Endl;
    env.SendSync(new NFake::TEvCompact(Table2Id));
    Cerr << "...waiting until second table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(Table2Id, key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 98, 13, 1}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 112);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // read from in-memory table, should be no more cache misses
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(TableId, key, retried), true);
    }

    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 112);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);

    RestartAndClearCache(env, 10_MB);

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(Table2Id, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 232);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // read from in-memory table, should be preloaded
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(TableId, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 232);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);
}

Y_UNIT_TEST(TryKeepInMemoryMode_Disabling) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);

    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    SetupSharedCache(env, 10_MB, true);

    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(TableId, true) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(TableId, key, std::move(value)) });
    }
    Cerr << "...compacting first table" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until first table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 119);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // Disable in-memory
    env.SendSync(new NFake::TEvExecute{ new TTxTryKeepInMemory(TableId, false) });
    
    // make second table to try to preempt first table from cache
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(Table2Id, false) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(Table2Id, key, std::move(value)) });
    }
    Cerr << "...compacting second table" << Endl;
    env.SendSync(new NFake::TEvCompact(Table2Id));
    Cerr << "...waiting until second table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(Table2Id, key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 127);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));

    // read from previously-in-memory table, should be preempted
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(TableId, key, retried), true);
    }

    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 98, 13, 1}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 125);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 2);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(10'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 112);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);

    RestartAndClearCache(env, 10_MB);

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(Table2Id, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 120);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(20'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 232);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));

    // read from previously-in-memory table, should not be preloaded
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(TableId, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 138);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(29_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 348);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);
}

Y_UNIT_TEST(TryKeepInMemoryMode_AfterCompaction) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    // x2 mem of in-mem table to prevent eviction during compaction
    SetupSharedCache(env, 20_MB, true);

    // count shared cache fetches of data pages of in-mem table
    ui64 inMemFetchesCount = 0;
    auto inMemFetchesObserver = env.Env.AddObserver<NBlockIO::TEvFetch>([&inMemFetchesCount] (const auto& ev) {
        // in-mem table will be created with channel 2
        if (ev->Get()->PageCollection->Label().Channel() == 2) {
            ++inMemFetchesCount;
        }
    });

    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(TableId, true, 2) });
    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(TableId, key, std::move(value)) });
    }
    Cerr << "...compacting first table" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until first table gen 0 compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until first table gen 1 compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();
    // generations 2+ are not preserved in memory in the default user table compaction policy
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until first table gen 2 compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(20_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 249);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(20'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 234);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // make second table to try to preempt first table from cache
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(Table2Id, false, 3) });
    // write 100 rows, each ~200KB (~20MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(200 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(Table2Id, key, std::move(value)) });
    }
    Cerr << "...compacting second table" << Endl;
    env.SendSync(new NFake::TEvCompact(Table2Id));
    Cerr << "...waiting until second table compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(Table2Id, key, retried), true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 49, 6}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(20_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 183);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(26_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 275);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(23'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 129);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // read from in-memory table, should be no more cache misses
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        DoReadRows(env, new TTxReadRows(TableId, key, retried), true);
    }

    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(20_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 183);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(26_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 275);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(23'000_KB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 129);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);

    // should not be fetches for in-mem table
    UNIT_ASSERT_VALUES_EQUAL(inMemFetchesCount, 0);

    RestartAndClearCache(env, 10_MB);

    // there are some fetches after restart and cache cleaning
    UNIT_ASSERT_VALUES_EQUAL(inMemFetchesCount, 2);

    // read from regular table, sould be some cache misses
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(Table2Id, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 137);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(26_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 275);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(42_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 249);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));

    // read from in-memory table, should be no more cache misses and all read should be from private cache (no more CacheHit*)
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(TableId, key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 137);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(26_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 275);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(42_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 249);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->TargetInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveInMemoryBytes->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissInMemoryBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissInMemoryPages->Val(), 0);

    // no more fetches
    UNIT_ASSERT_VALUES_EQUAL(inMemFetchesCount, 2);
}

}

Y_UNIT_TEST_SUITE(TSharedPageCache_Transactions) {

void BasicSetup(TMyEnvBase& env) {
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, 0_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();
}

void ManyPartsSetup(TMyEnvBase& env) {
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(false);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    TAutoPtr<TTxInitSchema> initSchema = new TTxInitSchema();
    initSchema->CompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
    initSchema->CompactionPolicy->InMemForceStepsToSnapshot = 1;
    for (auto& gen : initSchema->CompactionPolicy->Generations) {
        gen.ExtraCompactionPercent = 0;
        gen.ExtraCompactionMinSize = 100;
        gen.ExtraCompactionExpPercent = 0;
        gen.ExtraCompactionExpMaxSize = 0;
        gen.UpliftPartSize = 0;
    }
    env.SendSync(new NFake::TEvExecute{ initSchema });

    SetupSharedCache(env, 0_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }
}

Y_UNIT_TEST(One_Transaction_One_Key) {
    TMyEnvBase env;
    auto counters = GetSharedPageCounters(env);

    BasicSetup(env);

    Cerr << "...making read" << Endl;
    TRetriedCounters retried;
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(33, retried) });
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 4);
}

Y_UNIT_TEST(One_Transaction_Two_Keys) {
    TMyEnvBase env;
    auto counters = GetSharedPageCounters(env);

    BasicSetup(env);

    Cerr << "...making read" << Endl;
    TRetriedCounters retried;
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows({33, 66}, retried) });
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 7);
}

Y_UNIT_TEST(One_Transaction_Two_Keys_Many_Parts) {
    TMyEnvBase env;
    auto counters = GetSharedPageCounters(env);

    ManyPartsSetup(env);
    
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 96);

    Cerr << "...making read" << Endl;
    TRetriedCounters retried;
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows({33, 66}, retried) });
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 98);
}

Y_UNIT_TEST(Two_Transactions_One_Key) {
    TMyEnvBase env;
    auto counters = GetSharedPageCounters(env);

    BasicSetup(env);

    Cerr << "...making read" << Endl;

    TBlockEvents<NSharedCache::TEvRequest> block(env.Env);
            
    TRetriedCounters retried1, retried2;
    bool completed1, completed2;
    env.SendAsync(new NFake::TEvExecute{ new TTxReadRows({33}, retried1, completed1) });
    env.SendAsync(new NFake::TEvExecute{ new TTxReadRows({33}, retried2, completed2) });

    env.Env.WaitFor("blocked TEvRequest 1", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 2", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 2);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 4);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 6);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 4", [&]{ return completed1 && completed2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT(block.empty());
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 8);
}

Y_UNIT_TEST(Two_Transactions_Two_Keys) {
    TMyEnvBase env;
    auto counters = GetSharedPageCounters(env);

    BasicSetup(env);

    Cerr << "...making read" << Endl;

    TBlockEvents<NSharedCache::TEvRequest> block(env.Env);
            
    TRetriedCounters retried1, retried2;
    bool completed1, completed2;
    env.SendAsync(new NFake::TEvExecute{ new TTxReadRows({33}, retried1, completed1) });
    env.SendAsync(new NFake::TEvExecute{ new TTxReadRows({66}, retried2, completed2) });

    // b-tree index root is common page:
    env.Env.WaitFor("blocked TEvRequest 1", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 2", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 2);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 4);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 6);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 4", [&]{ return completed1 && completed2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT(block.empty());
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 8);
}

Y_UNIT_TEST(Compaction) {
    TMyEnvBase env;
    auto counters = GetSharedPageCounters(env);

    BasicSetup(env);

    Cerr << "...making read" << Endl;

    TBlockEvents<NSharedCache::TEvRequest> block(env.Env);
            
    TRetriedCounters retried;
    bool completed;
    env.SendAsync(new NFake::TEvExecute{ new TTxReadRows({33}, retried, completed) });

    env.Env.WaitFor("blocked TEvRequest 1", [&]{ return block.size() == 1; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(completed, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);
    
    block.Stop();

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    block.Unblock();

    // page collection invalidation activates waiting transactions:
    env.Env.WaitFor("blocked TEvRequest 2", [&]{ return completed; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 122);

    env->SimulateSleep(TDuration::Seconds(3));
    // nothing should crash    
}

}

}
