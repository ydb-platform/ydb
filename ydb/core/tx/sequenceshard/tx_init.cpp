#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxInit : public TTxBase {
        explicit TTxInit(TSelf* self)
            : TTxBase(self)
        { }

        TTxType GetTxType() const override { return TXTYPE_INIT; }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            SLOG_T("TTxInit.Execute");

            Reset();

            NIceDb::TNiceDb db(txc.DB);
            bool ok = true;
            ok &= LoadSysParams(db);
            ok &= LoadSequences(db);
            ok &= LoadSchemeShardRounds(db);

            if (!ok) {
                return false;
            }

            Self->SwitchToWork(ctx);
            return true;
        }

        void Reset() {
            Self->ResetState();
            Self->ResetCounters();
        }

        bool LoadSysParams(NIceDb::TNiceDb& db) {
            auto rowset = db.Table<Schema::SysParams>().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                auto id = Schema::ESysParam(rowset.GetValue<Schema::SysParams::Id>());
                switch (id) {
                    case Schema::SysParam_SchemeShardId: {
                        Self->CurrentSchemeShardId = rowset.GetValue<Schema::SysParams::IntValue>();
                        break;
                    }
                    case Schema::SysParam_ProcessingParams: {
                        auto binary = rowset.GetValue<Schema::SysParams::BinaryValue>();
                        Y_ABORT_UNLESS(Self->ProcessingParams.emplace().ParseFromString(binary));
                        break;
                    }
                    default: {
                        // ignore unknown values
                        break;
                    }
                }

                if (!rowset.Next())
                    return false;
            }

            return true;
        }

        bool LoadSequences(NIceDb::TNiceDb& db) {
            auto rowset = db.Table<Schema::Sequences>().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TPathId pathId(
                    rowset.GetValue<Schema::Sequences::OwnerId>(),
                    rowset.GetValue<Schema::Sequences::LocalId>());
                auto& sequence = Self->Sequences[pathId];
                sequence.PathId = pathId;
                sequence.MinValue = rowset.GetValue<Schema::Sequences::MinValue>();
                sequence.MaxValue = rowset.GetValue<Schema::Sequences::MaxValue>();
                sequence.StartValue = rowset.GetValue<Schema::Sequences::StartValue>();
                sequence.NextValue = rowset.GetValue<Schema::Sequences::NextValue>();
                sequence.NextUsed = rowset.GetValue<Schema::Sequences::NextUsed>();
                sequence.Cache = rowset.GetValue<Schema::Sequences::Cache>();
                sequence.Increment = rowset.GetValue<Schema::Sequences::Increment>();
                sequence.Cycle = rowset.GetValue<Schema::Sequences::Cycle>();
                sequence.State = rowset.GetValue<Schema::Sequences::State>();
                sequence.MovedTo = rowset.GetValue<Schema::Sequences::MovedTo>();

                if (!rowset.Next())
                    return false;
            }

            return true;
        }

        bool LoadSchemeShardRounds(NIceDb::TNiceDb& db) {
            auto rowset = db.Table<Schema::SchemeShardRounds>().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                auto schemeShardId = rowset.GetValue<Schema::SchemeShardRounds::SchemeShardId>();
                auto generation = rowset.GetValue<Schema::SchemeShardRounds::Generation>();
                auto round = rowset.GetValue<Schema::SchemeShardRounds::Round>();
                Self->SchemeShardRounds[schemeShardId] = { generation, round };

                if (!rowset.Next())
                    return false;
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            SLOG_T("TTxInit.Complete");
        }
    };

    void TSequenceShard::RunTxInit(const TActorContext& ctx) {
        Execute(new TTxInit(this), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
