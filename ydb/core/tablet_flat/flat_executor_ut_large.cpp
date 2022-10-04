#include "flat_executor_ut_common.h"

#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TFlatTableLongTxLarge) {

    enum : ui32  {
        TableId = 101,
        KeyColumnId = 1,
        ValueColumnId = 2,
        Value2ColumnId = 3,
    };

    struct TTxInitSchema : public ITransaction {
        TTxInitSchema(TIntrusiveConstPtr<TCompactionPolicy> policy = nullptr)
            : Policy(std::move(policy))
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (txc.DB.GetScheme().GetTableInfo(TableId))
                return true;

            txc.DB.Alter()
                .AddTable("test" + ToString(ui32(TableId)), TableId)
                .AddColumn(TableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false)
                .AddColumn(TableId, "value", ValueColumnId, NScheme::TString::TypeId, false)
                .AddColumn(TableId, "value2", Value2ColumnId, NScheme::TString::TypeId, false)
                .AddColumnToKey(TableId, KeyColumnId);

            if (Policy) {
                txc.DB.Alter().SetCompactionPolicy(TableId, *Policy);
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

        const TIntrusiveConstPtr<TCompactionPolicy> Policy;
    };

    struct TTxCommitLongTx : public ITransaction {
        ui64 TxId;
        TRowVersion RowVersion;

        explicit TTxCommitLongTx(ui64 txId, TRowVersion rowVersion = TRowVersion::Min())
            : TxId(txId)
            , RowVersion(rowVersion)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            txc.DB.CommitTx(TableId, TxId, RowVersion);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    template<ui32 ColumnId>
    struct TTxWriteRow : public ITransaction {
        i64 Key;
        TString Value;
        ui64 TxId;

        explicit TTxWriteRow(i64 key, TString value, ui64 txId = 0)
            : Key(key)
            , Value(std::move(value))
            , TxId(txId)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            const auto key = NScheme::TInt64::TInstance(Key);

            const auto val = NScheme::TString::TInstance(Value);
            NTable::TUpdateOp ops{ ColumnId, NTable::ECellOp::Set, val };

            if (TxId == 0) {
                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, { ops });
            } else {
                txc.DB.UpdateTx(TableId, NTable::ERowOp::Upsert, { key }, { ops }, TxId);
            }
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
            return true;
        }

        void Complete(const TActorContext&) override {
            // nothing
        }
    };

    struct TTxCheckRows : public ITransaction {
        TString& Data;

        TTxCheckRows(TString& data)
            : Data(data)
        { }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TStringBuilder builder;

            TVector<NTable::TTag> tags;
            tags.push_back(KeyColumnId);
            tags.push_back(ValueColumnId);
            tags.push_back(Value2ColumnId);

            NTable::EReady ready;
            auto it = txc.DB.IterateRange(TableId, { }, tags);

            while ((ready = it->Next(NTable::ENext::All)) != NTable::EReady::Gone) {
                if (ready == NTable::EReady::Page) {
                    return false;
                }

                const auto& row = it->Row();

                TString key;
                DbgPrintValue(key, row.Get(0), NScheme::TTypeInfo(NScheme::TUint64::TypeId));

                TString value;
                DbgPrintValue(value, row.Get(1), NScheme::TTypeInfo(NScheme::TString::TypeId));

                TString value2;
                DbgPrintValue(value2, row.Get(2), NScheme::TTypeInfo(NScheme::TString::TypeId));

                builder << "Key " << key << " = " << row.GetRowState()
                    << " value = " << NTable::ECellOp(row.GetCellOp(1)) << " " << value
                    << " value2 = " << NTable::ECellOp(row.GetCellOp(2)) << " " << value2 << Endl;
            }

            Data = builder;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

// Note: this test requires too much memory when sanitizers are enabled
#if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
    Y_UNIT_TEST(LargeDeltaChain) {
        TMyEnvBase env;

        //env->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NActors::NLog::PRI_DEBUG);
        //env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        env.FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));

        // A special compaction policy that compacts small mem tables often
        // However the data will not be compacted and not kept in the cache
        // This is needed to limit the memory pressure for this test
        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy;
        policy->InMemSizeToSnapshot = 24ull * 1024 * 1024;
        policy->InMemStepsToSnapshot = 4;
        policy->InMemForceStepsToSnapshot = 4;
        policy->InMemForceSizeToSnapshot = 24ull * 1024 * 1024;
        policy->Generations.push_back({10ull * 1024 * 1024 * 1024, 1024, 1024, 10ull * 1024 * 1024 * 1024, NLocalDb::LegacyQueueIdToTaskName(1), false});

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema(policy) });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(1, "foo") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(2, "bar") });
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<ValueColumnId>(3, "baz") });

        // Now start 1024 transactions, each making a 4.1MB update for key 2
        for (ui64 txId = 1; txId < 1024; ++txId) {
            TString value(size_t(4.1 * 1024 * 1024), char('a' + txId % 26));
            env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, std::move(value), txId) });
        }

        // The last transaction will be eventually committed
        env.SendSync(new NFake::TEvExecute{ new TTxWriteRow<Value2ColumnId>(2, "hello", 1024) });

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Set bar value2 = Empty NULL\n"
                "Key 3 = Upsert value = Set baz value2 = Empty NULL\n");
        }

        // Commit the last transaction in the chain
        env.SendSync(new NFake::TEvExecute{ new TTxCommitLongTx(1024) });

        // Force a full compaction of a table
        Cerr << "...compacting" << Endl;
        env.SendSync(new NFake::TEvCompact(TableId));
        Cerr << "...waiting until compacted" << Endl;
        env.WaitFor<NFake::TEvCompacted>();

        {
            TString data;
            env.SendSync(new NFake::TEvExecute{ new TTxCheckRows(data) });
            UNIT_ASSERT_VALUES_EQUAL(data,
                "Key 1 = Upsert value = Set foo value2 = Empty NULL\n"
                "Key 2 = Upsert value = Set bar value2 = Set hello\n"
                "Key 3 = Upsert value = Set baz value2 = Empty NULL\n");
        }
    }
#endif
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr
