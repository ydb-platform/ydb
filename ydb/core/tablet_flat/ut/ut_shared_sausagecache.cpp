#include "flat_executor_ut_common.h"

#include <util/system/sanitizers.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/testlib/actors/block_events.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;

enum : ui32  {
    TableId = 101,
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

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

    NLocalDb::TCompactionPolicyPtr CompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
};

struct TTxWriteRow : public ITransaction {
    i64 Key;
    TString Value;

    explicit TTxWriteRow(i64 key, TString value)
        : Key(key)
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
};

struct TTxReadRows : public ITransaction {
    TVector<i64> Keys;
    TRetriedCounters& Retried;
    ui32 Attempts = 0;
    bool& Completed;

    TTxReadRows(TVector<i64> keys, TRetriedCounters& retried, bool& completed)
        : Keys(keys)
        , Retried(retried)
        , Completed(completed)
    { 
        Completed = false;
    }

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

void SetupSharedCache(TMyEnvBase& env, NKikimrSharedCache::TReplacementPolicy policy = NKikimrSharedCache::ThreeLeveledLRU, ui64 limit = 8_MB, bool resetMemoryLimit = false) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    auto config = request->Record.MutableConfig()->MutableSharedCacheConfig();
    config->SetReplacementPolicy(policy);
    config->SetMemoryLimit(limit);
    
    env->Send(MakeSharedPageCacheId(), TActorId{}, request.Release());
    WaitEvent(env, NConsole::TEvConsole::EvConfigNotificationRequest);

    if (resetMemoryLimit) {
        env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(Max<ui64>()));
        WaitEvent(env, NMemory::EvConsumerLimit);
    }
}

Y_UNIT_TEST_SUITE(TSharedPageCache) {

Y_UNIT_TEST(Limits) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    bool bTreeIndex = env->GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();
    ui32 passiveBytes = bTreeIndex ? 131 : 7772;

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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
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
    ui32 passiveBytes = bTreeIndex ? 131 : 7772;

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, NKikimrSharedCache::ThreeLeveledLRU);

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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->LoadInFlyBytes->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 8_MB);

    SetupSharedCache(env, NKikimrSharedCache::ThreeLeveledLRU, 100_MB);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 100_MB);

    SetupSharedCache(env, NKikimrSharedCache::ThreeLeveledLRU, 2_MB);
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

    SetupSharedCache(env, NKikimrSharedCache::ThreeLeveledLRU, 0_MB);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->ConfigLimitBytes->Val(), 0_MB);
}

Y_UNIT_TEST(ThreeLeveledLRU) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, NKikimrSharedCache::ThreeLeveledLRU, 8_MB, true);

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
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 45, 5}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 44, 6}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 73, 10, 1}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 100, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14}));
}

Y_UNIT_TEST(S3FIFO) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 8_MB, true);

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
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 20, 3}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 23, 2}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 100, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 28, 4, 1}));
}

Y_UNIT_TEST(ClockPro) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, NKikimrSharedCache::ClockPro, 8_MB, true);

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
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 21, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 20, 2}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));

    // simulate scan again
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13}));

    // read the key again again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRows(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 100, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 1}));
}

Y_UNIT_TEST(ReplacementPolicySwitch) {
    TMyEnvBase env;
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    auto counters = GetSharedPageCounters(env);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, NKikimrSharedCache::ThreeLeveledLRU, 8_MB, true);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    RestartAndClearCache(env);

    TRetriedCounters retried = {};
    for (i64 key = 0; key < 3; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{3, 3, 1, 1}));

    UNIT_ASSERT_GT(counters->ReplacementPolicySize(NKikimrSharedCache::ThreeLeveledLRU)->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 0);

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 8_MB);

    retried = {};
    for (i64 key = 0; key < 3; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{3}));

    retried = {};
    for (i64 key = 90; key < 93; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
    }
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{3, 3, 2, 1}));

    UNIT_ASSERT_GT(counters->ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->ReplacementPolicySize(NKikimrSharedCache::ThreeLeveledLRU)->Val(), 0);
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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 20_MB, true);

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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 20_MB, true);

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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 8_MB, true);

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
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 98);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 8_MB, true);

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
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 83);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19}));
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActivePages->Val(), 82);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->PassiveBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->PassivePages->Val(), 1);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheHitBytes->Val(), static_cast<i64>(0_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheHitPages->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->CacheMissBytes->Val(), static_cast<i64>(2_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 19);

    RestartAndClearCache(env);
    LogCounters(counters);
    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 0_MB, true);

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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 0_MB, true);

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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRows(key, retried) }, true);
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

}

Y_UNIT_TEST_SUITE(TSharedPageCache_WaitPads) {

void BasicSetup(TMyEnvBase& env) {
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_TRACE);
    env->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 0_MB, true);

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

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 0_MB, true);

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

    // CHANGE LATER: block.size() == 2
    env.Env.WaitFor("blocked TEvRequest 1", [&]{ return block.size() == 1; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 0);

    block.Unblock();
    // CHANGE LATER: block.size() == 2
    env.Env.WaitFor("blocked TEvRequest 2", [&]{ return block.size() == 1; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    // CHANGE LATER: counters->CacheMissPages->Val() == 2
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 1);

    block.Unblock();
    // CHANGE LATER: block.size() == 2
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 1; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    // CHANGE LATER: counters->CacheMissPages->Val() == 4
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 2);

    block.Unblock();
    // CHANGE LATER: block.size() == 2
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 1; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    // CHANGE LATER: counters->CacheMissPages->Val() == 6
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 3);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 4", [&]{ return completed1 && completed2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT(block.empty());
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1, 1}));
    // CHANGE LATER: counters->CacheMissPages->Val() == 8
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 4);
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
    // CHANGE LATER: block.size() == 2
    env.Env.WaitFor("blocked TEvRequest 1", [&]{ return block.size() == 1; }, TDuration::Seconds(10));
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
    // CHANGE LATER: counters->CacheMissPages->Val() == 1
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 1);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    // CHANGE LATER: counters->CacheMissPages->Val() == 4
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 3);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 3", [&]{ return block.size() == 2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(completed1, false);
    UNIT_ASSERT_VALUES_EQUAL(completed2, false);
    // CHANGE LATER: counters->CacheMissPages->Val() == 6
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 5);

    block.Unblock();
    env.Env.WaitFor("blocked TEvRequest 4", [&]{ return completed1 && completed2; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT(block.empty());
    UNIT_ASSERT_VALUES_EQUAL(retried1, (TVector<ui32>{1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(retried2, (TVector<ui32>{1, 1, 1, 1, 1}));
    // CHANGE LATER: counters->CacheMissPages->Val() == 8
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 7);
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

    // page collection invalidation activates waiting transactions:
    env.Env.WaitFor("blocked TEvRequest 2", [&]{ return completed; }, TDuration::Seconds(10));
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1, 1, 1}));
    UNIT_ASSERT_VALUES_EQUAL(counters->CacheMissPages->Val(), 121);

    block.Unblock();
    env->SimulateSleep(TDuration::Seconds(3));
    // nothing should crash    
}

}

}
