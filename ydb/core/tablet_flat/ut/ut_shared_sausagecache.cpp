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

} // Y_UNIT_TEST_SUITE(TSharedPageCache)

} // namespace NTabletFlatExecutor
} // namespace NKikimr
