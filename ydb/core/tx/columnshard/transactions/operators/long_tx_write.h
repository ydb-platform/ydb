#pragma once

#include "propose_tx.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

    class TLongTxTransactionOperator: public IProposeTxOperator {
        using TBase = IProposeTxOperator;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TLongTxTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT);

    private:
        virtual TString DoDebugString() const override {
            return "LONG_TX_WRITE";
        }

        virtual TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
        virtual void DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {

        }
        virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
        }
        virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
        }
        virtual bool DoIsAsync() const override {
            return false;
        }
        virtual bool DoParse(TColumnShard& owner, const TString& data) override;
        virtual bool DoCheckTxInfoForReply(const TFullTxInfo& /*originalTxInfo*/) const override {
            return true;
        }
        virtual bool DoCheckAllowUpdate(const TFullTxInfo& currentTxInfo) const override {
            return (currentTxInfo.Source == GetTxInfo().Source && currentTxInfo.Cookie == GetTxInfo().Cookie);
        }
    public:
        using TBase::TBase;

        virtual void DoOnTabletInit(TColumnShard& owner) override {
            for (auto&& writeId : WriteIds) {
                AFL_VERIFY(owner.LongTxWrites.contains(writeId))("problem", "ltx_not_exists_for_write_id")("txId", GetTxId())("writeId", (ui64)writeId);
                owner.AddLongTxWrite(writeId, GetTxId());
            }
        }

        bool ProgressOnExecute(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
            TBlobGroupSelector dsGroupSelector(owner.Info());
            NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

            auto pathExists = [&](ui64 pathId) {
                return owner.TablesManager.HasTable(pathId);
            };

            auto counters = owner.InsertTable->Commit(dbTable, version.GetPlanStep(), version.GetTxId(), WriteIds, pathExists);

            owner.Counters.GetTabletCounters().IncCounter(COUNTER_BLOBS_COMMITTED, counters.Rows);
            owner.Counters.GetTabletCounters().IncCounter(COUNTER_BYTES_COMMITTED, counters.Bytes);
            owner.Counters.GetTabletCounters().IncCounter(COUNTER_RAW_BYTES_COMMITTED, counters.RawBytes);

            NIceDb::TNiceDb db(txc.DB);
            for (TWriteId writeId : WriteIds) {
                AFL_VERIFY(owner.RemoveLongTxWrite(db, writeId, GetTxId()));
            }
            owner.UpdateInsertTableCounters();
            return true;
        }

        bool ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) override {
            auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(owner.TabletID(), TxInfo.TxKind, GetTxId(), NKikimrTxColumnShard::SUCCESS);
            result->Record.SetStep(TxInfo.PlanStep);
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
            return true;
        }

        virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            NIceDb::TNiceDb db(txc.DB);
            for (TWriteId writeId : WriteIds) {
                AFL_VERIFY(owner.RemoveLongTxWrite(db, writeId, GetTxId()));
            }
            TBlobGroupSelector dsGroupSelector(owner.Info());
            NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
            owner.InsertTable->Abort(dbTable, WriteIds);
            return true;
        }
        virtual bool CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
            return true;
        }

    private:
        THashSet<TWriteId> WriteIds;
    };

}   // namespace NKikimr::NColumnShard
