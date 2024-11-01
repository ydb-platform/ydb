#include "flat_executor_ut_common.h"

#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TSharedPageCache) {

constexpr i64 MB = 1024*1024;

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
        << ", MemTable:" << counters->MemTableCompactingBytes->Val() << "/" << counters->MemTableTotalBytes->Val()
        << Endl;
}

void WaitEvent(TMyEnvBase& env, ui32 eventType, ui32 requiredCount = 1) {
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(eventType, requiredCount));
    env->DispatchEvents(options);
}

void RestartAndClearCache(TMyEnvBase& env) {
    env.SendSync(new TEvents::TEvPoison, false, true);

    env->GetMemObserver()->NotifyStat({200*MB, 100*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);

    env->GetMemObserver()->NotifyStat({100*MB, 108*MB, 108*MB});
    WaitEvent(env, NSharedCache::EvMem);

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
}

void SwitchPolicy(TMyEnvBase& env, NKikimrSharedCache::TReplacementPolicy policy) {
    auto configure = MakeHolder<TEvSharedPageCache::TEvConfigure>();
    configure->Record.SetReplacementPolicy(policy);
    configure->Record.SetMemoryLimit(0); // no limit
    env->Send(MakeSharedPageCacheId(), TActorId{}, configure.Release());
    WaitEvent(env, TEvSharedPageCache::EvConfigure);
}

Y_UNIT_TEST(Limits) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    bool bTreeIndex = env->GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();
    ui32 passiveBytes = bTreeIndex ? 131 : 7772;

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    auto configure = MakeHolder<TEvSharedPageCache::TEvConfigure>();
    configure->Record.SetMemoryLimit(8_MB);
    env->Send(MakeSharedPageCacheId(), TActorId{}, configure.Release());
    WaitEvent(env, TEvSharedPageCache::EvConfigure);

    SwitchPolicy(env, NKikimrSharedCache::ThreeLeveledLRU);

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
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 3); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 0);

    env->GetMemObserver()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 5*MB + counters->ActiveBytes->Val() + counters->PassiveBytes->Val());

    env->GetMemObserver()->NotifyStat({101*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2 - MB, MB / 3); // 1mb evicted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB / 3 * 2 - MB, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), counters->ActiveLimitBytes->Val());

    env->GetMemObserver()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2 - MB, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 5*MB + counters->ActiveBytes->Val() + counters->PassiveBytes->Val()); // +5mb

    env->GetMemObserver()->NotifyStat({200*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 1); // zero limit
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 1); // zero limit
}

Y_UNIT_TEST(MemTableRegistration) {
    TMyEnvBase env;

    // start:

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendAsync(new NFake::TEvExecute{ new TTxInitSchema() });

    WaitEvent(env, NSharedCache::EvMemTableRegistered);
    env.WaitForWakeUp(1);

    // stop:

    env.SendAsync(new TEvents::TEvPoison);
    
    WaitEvent(env, NSharedCache::EvUnregister);

    // restart:

    env.FireTablet(env.Edge, env.Tablet, [&](const TActorId &tablet, TTabletStorageInfo *info) {
        return new NFake::TDummy(tablet, info, env.Edge, 0);
    });

    WaitEvent(env, NSharedCache::EvMemTableRegistered);
}

Y_UNIT_TEST(MemTableLimits) {
    TMyEnvBase env;

    env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_DEBUG);

    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    TVector<ui64> tabletIds;
    for (size_t tablet = 0; tablet < 10; tablet++) {
        tabletIds.push_back(env.Tablet);
        env.Tablet++;
    }

    for (auto tabletId : tabletIds) {
        env.Tablet = tabletId;

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

        // write 10 rows, each ~100KB
        for (i64 key = 0; key < 10; ++key) {
            TString value(size_t(100 * 1024), char('a' + key % 26));
            env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
        }

        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        TRetriedCounters retried;
        for (i64 key = 0; key < 10; ++key) {
            env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) });
        }

        // write 10 rows, each ~50KB
        for (i64 key = 100; key < 110; ++key) {
            TString value(size_t(50 * 1024), char('a' + key % 26));
            env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
        }
    }

    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 5*MB, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemTableCompactingBytes->Val(), 0);

    env->GetMemObserver()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_GE(counters->MemLimitBytes->Val(), 8*MB); // bigger than config value
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 5*MB, MB / 3);

    env->GetMemObserver()->NotifyStat({100*MB + 1, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB / 3 * 2, MB / 3); // 1 page evicted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemLimitBytes->Val(), 8*MB / 3 * 2, MB / 3);
    
    // we want to get ~2.7MB back, it's 6 Mem Tables
    env.WaitFor<NFake::TEvCompacted>(6);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 2*MB, MB / 3);

    env->GetMemObserver()->NotifyStat({200*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 1); // zero limit
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 1); // zero limit
    
    // compacted all except reservation, it's all except 20% of 8MB = 1.6MB = 3 Mem Tables
    env.WaitFor<NFake::TEvCompacted>(1); // so 1 more Mem Table compacted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 1.5*MB, MB / 3);
}

Y_UNIT_TEST(ThreeLeveledLRU) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SwitchPolicy(env, NKikimrSharedCache::ThreeLeveledLRU);
    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);

    env->GetMemObserver()->NotifyStat({100*MB, 108*MB, 108*MB});
    WaitEvent(env, NSharedCache::EvMem);

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

    SwitchPolicy(env, NKikimrSharedCache::S3FIFO);
    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);

    env->GetMemObserver()->NotifyStat({100*MB, 108*MB, 108*MB});
    WaitEvent(env, NSharedCache::EvMem);

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
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 19, 2}));

    retried = {};
    for (i64 key = 99; key >= 0; --key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key, retried) }, true);
    }
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB / 3));
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 21, 3}));

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
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    SwitchPolicy(env, NKikimrSharedCache::ClockPro);
    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);

    env->GetMemObserver()->NotifyStat({100*MB, 108*MB, 108*MB});
    WaitEvent(env, NSharedCache::EvMem);

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
    UNIT_ASSERT_VALUES_EQUAL(retried, (TVector<ui32>{100, 20, 2}));

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
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });
    env->GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(true);

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

    SwitchPolicy(env, NKikimrSharedCache::S3FIFO);

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

} // Y_UNIT_TEST_SUITE(TSharedPageCache)

} // namespace NTabletFlatExecutor
} // namespace NKikimr
