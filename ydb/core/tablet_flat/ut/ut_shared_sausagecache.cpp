#include "flat_executor_ut_common.h"

#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

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

void LogCounters(TIntrusivePtr<TSharedPageCacheCounters> counters) {
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

void RestartAndClearCache(TMyEnvBase& env) {
    env.SendSync(new TEvents::TEvPoison, false, true);
    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(8_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
}

Y_UNIT_TEST(Limits) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    bool bTreeIndex = env->GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();
    ui32 passiveBytes = bTreeIndex ? 131 : 7772;

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

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
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3)); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 0);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(100_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 100_MB);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(6_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 3 * 2), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 6_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 6_MB);

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(3_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(3_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 3_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), counters->ActiveLimitBytes->Val());

    env->Send(MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0_MB));
    WaitEvent(env, NMemory::EvConsumerLimit);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), counters->ActiveLimitBytes->Val());
}

Y_UNIT_TEST(ThreeLeveledLRU) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

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
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    env->Send(MakeSharedPageCacheId(), TActorId{}, new TEvReplacementPolicySwitch(NKikimrSharedCache::S3FIFO));
    WaitEvent(env, NSharedCache::EvReplacementPolicySwitch);

    // write 100 rows, each ~100KB (~10MB)
    for (i64 key = 0; key < 100; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    TRetriedCounters retried;
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 92, 12}));

    RestartAndClearCache(env);

    retried = {};
    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 10), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 100, 14, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 92}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 28}));

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
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB / 10), static_cast<i64>(1_MB / 3));
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
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 28}));
}

Y_UNIT_TEST(ReplacementPolicySwitch) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

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

    env->Send(MakeSharedPageCacheId(), TActorId{}, new TEvReplacementPolicySwitch(NKikimrSharedCache::S3FIFO));
    WaitEvent(env, NSharedCache::EvReplacementPolicySwitch);

    retried = {};
    for (i64 key = 0; key < 3; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{3}));
}

} // Y_UNIT_TEST_SUITE(TSharedPageCache)

} // namespace NTabletFlatExecutor
} // namespace NKikimr
