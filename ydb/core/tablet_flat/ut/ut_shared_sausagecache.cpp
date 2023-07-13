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
    Cerr << "Counters: Active:" << counters->ActiveBytes->Val() << "/" << counters->ActiveLimitBytes->Val() << ", Passive:" << counters->PassiveBytes->Val() << ", MemLimit:" << counters->MemLimitBytes->Val() << Endl;
}

void WaitMemEvent(TMyEnvBase& env) {
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(NSharedCache::EvMem, 1));
    env->DispatchEvents(options);
}

Y_UNIT_TEST(Limits) {
    TMyEnvBase env;
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(env->GetDynamicCounters());

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
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 2); // 2 full layers (fresh & staging)
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 7772);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 0);

    env->GetMemObserver()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitMemEvent(env);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2, MB / 2);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 7772);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 5*MB + counters->ActiveBytes->Val() + counters->PassiveBytes->Val());


    env->GetMemObserver()->NotifyStat({101*MB, 110*MB, 100*MB});
    WaitMemEvent(env);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2 - MB, MB / 2); // 1mb evicted
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB / 3 * 2 - MB, MB / 2);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 7772);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), counters->ActiveLimitBytes->Val());

    env->GetMemObserver()->NotifyStat({95*MB, 110*MB, 100*MB});
    WaitMemEvent(env);
    LogCounters(counters);
    UNIT_ASSERT_DOUBLES_EQUAL(counters->ActiveBytes->Val(), 8*MB / 3 * 2 - MB, MB / 2);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 8*MB);
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 7772);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 5*MB + counters->ActiveBytes->Val() + counters->PassiveBytes->Val()); // +5mb

    env->GetMemObserver()->NotifyStat({200*MB, 110*MB, 100*MB});
    WaitMemEvent(env);
    LogCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->ActiveLimitBytes->Val(), 1); // zero limit
    UNIT_ASSERT_VALUES_EQUAL(counters->PassiveBytes->Val(), 7772);
    UNIT_ASSERT_VALUES_EQUAL(counters->MemLimitBytes->Val(), 1); // zero limit
}

} // Y_UNIT_TEST_SUITE(TSharedPageCache)

} // namespace NTabletFlatExecutor
} // namespace NKikimr
