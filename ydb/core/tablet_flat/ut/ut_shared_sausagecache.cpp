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

struct TTxInitSchema : public ITransaction {
    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (txc.DB.GetScheme().GetTableInfo(TableId))
            return true;

        txc.DB.Alter()
            .AddTable("test" + ToString(ui32(TableId)), TableId)
            .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
            .AddColumn(TableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
            .AddColumnToKey(TableId, KeyColumnId);

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

    explicit TTxReadRow(i64 key)
        : Key(key)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TVector<TRawTypeValue> rawKey;
        rawKey.emplace_back(&Key, sizeof(Key), NScheme::TTypeInfo(NScheme::TInt64::TypeId));

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

Y_UNIT_TEST(PageCacheLimits) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

    bool bTreeIndex = env->GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();
    ui32 passiveBytes = bTreeIndex ? 131 : 7772;

    env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
    env.SendSync(new NFake::TEvExecute{ new TTxInitSchema() });

    // write 300 rows, each ~100KB
    for (i64 key = 0; key < 300; ++key) {
        TString value(size_t(100 * 1024), char('a' + key % 26));
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
    }

    Cerr << "...compacting" << Endl;
    env.SendSync(new NFake::TEvCompact(TableId));
    Cerr << "...waiting until compacted" << Endl;
    env.WaitFor<NFake::TEvCompacted>();

    for (i64 key = 0; key < 100; ++key) {
        env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key) });
    }
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->LoadInFlyBytes->Val(), 0);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 3); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 0);

    env->GetMemoryConsumers()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 5*MB + counters->ActiveBytes->Val() + counters->PassiveBytes->Val());

    env->GetMemoryConsumers()->NotifyStat({101*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2 - MB, MB / 3); // 1mb evicted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB / 3 * 2 - MB, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), counters->ActiveLimitBytes->Val());

    env->GetMemoryConsumers()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2 - MB, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), passiveBytes);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 5*MB + counters->ActiveBytes->Val() + counters->PassiveBytes->Val()); // +5mb

    env->GetMemoryConsumers()->NotifyStat({200*MB, 110*MB, 100*MB});
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

        for (i64 key = 0; key < 10; ++key) {
            env.SendSync(new NFake::TEvExecute{ new TTxReadRow(key) });
        }

        // write 10 rows, each ~50KB
        for (i64 key = 100; key < 110; ++key) {
            TString value(size_t(50 * 1024), char('a' + key % 26));
            env.SendSync(new NFake::TEvExecute{ new TTxWriteRow(key, std::move(value)) });
        }
    }

    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 5*MB, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemTableCompactingBytes->Val(), 0);

    env->GetMemoryConsumers()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 3);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_GE(counters->MemLimitBytes->Val(), 8*MB); // bigger than config value
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 5*MB, MB / 3);

    env->GetMemoryConsumers()->NotifyStat({100*MB + 1, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB / 3 * 2, MB / 3); // 1 page evicted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemLimitBytes->Val(), 8*MB / 3 * 2, MB / 3);
    
    // we want to get ~2.7MB back, it's 6 Mem Tables
    env.WaitFor<NFake::TEvCompacted>(6);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 2*MB, MB / 3);

    env->GetMemoryConsumers()->NotifyStat({200*MB, 110*MB, 100*MB});
    WaitEvent(env, NSharedCache::EvMem);
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 1); // zero limit
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 1); // zero limit
    
    // compacted all except reservation, it's all except 20% of 8MB = 1.6MB = 3 Mem Tables
    env.WaitFor<NFake::TEvCompacted>(1); // so 1 more Mem Table compacted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->MemTableTotalBytes->Val(), 1.5*MB, MB / 3);
}

} // Y_UNIT_TEST_SUITE(TSharedPageCache)

} // namespace NTabletFlatExecutor
} // namespace NKikimr
