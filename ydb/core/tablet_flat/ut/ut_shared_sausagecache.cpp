#include "flat_executor_ut_common.h"

#include <util/system/sanitizers.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;

Y_UNIT_TEST_SUITE(TSharedPageCache) {

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

        TCompactionPolicy policy;
        policy.MinBTreeIndexNodeSize = 128;

        txc.DB.Alter()
            .AddTable("test" + ToString(ui32(TableId)), TableId)
            .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
            .AddColumn(TableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
            .AddColumnToKey(TableId, KeyColumnId)
            .SetCompactionPolicy(TableId, policy);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
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

struct TTxReadRow : public ITransaction {
    i64 Key;
    TRetriedCounters& Retried;
    ui32 Attempts = 0;

    explicit TTxReadRow(i64 key, TRetriedCounters& retried)
        : Key(key)
        , Retried(retried)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Increment(Retried, Attempts);
        Attempts++;

        TVector<TRawTypeValue> rawKey;
        rawKey.emplace_back(&Key, sizeof(Key), NScheme::TInt64::TypeId);

        TVector<NTable::TTag> tags;
        tags.push_back(KeyColumnId);
        tags.push_back(ValueColumnId);

        NTable::TRowState row;
        auto ready = txc.DB.Select(TableId, rawKey, tags, row);
        if (ready == NTable::EReady::Page) {
            return false;
        }

        return true;
    }

    void Complete(const TActorContext&ctx) override {
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 45, 5}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 44, 6}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 73, 10, 1}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 100, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 20, 3}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 23, 2}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 100, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 21, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 20, 2}));

    RestartAndClearCache(env);

    // read some key twice
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1}));

    // simulate scan
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13, 1}));

    // read the key again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));

    // simulate scan again
    retried = {};
    for (i64 key = 1; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{99, 99, 13}));

    // read the key again again
    retried = {};
    env.SendSync(new NFake::TEvExecute{ new TTxReadRow(0, retried) }, true);
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{1, 1, 1, 1}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{200, 100, 14, 2}));

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{3, 3, 1, 1}));

    UNIT_ASSERT_GT(counters->ReplacementPolicySize(NKikimrSharedCache::ThreeLeveledLRU)->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 0);

    SetupSharedCache(env, NKikimrSharedCache::S3FIFO, 8_MB);

    retried = {};
    for (i64 key = 0; key < 3; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{3}));

    retried = {};
    for (i64 key = 90; key < 93; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
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
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
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

}
