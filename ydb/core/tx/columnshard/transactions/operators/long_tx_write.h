#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

    class TLongTxTransactionOperator : public TTxController::ITransactionOperatior {
        using TBase = TTxController::ITransactionOperatior;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TLongTxTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT);
    public:
        using TBase::TBase;

        bool Parse(const TString& data) override {
            NKikimrTxColumnShard::TCommitTxBody commitTxBody;
            if (!commitTxBody.ParseFromString(data)) {
                return false;
            }

            for (auto& id : commitTxBody.GetWriteIds()) {
                WriteIds.insert(TWriteId{id});
            }
            return true;
        }

        void OnTabletInit(TColumnShard& owner) override {
           for (auto&& writeId : WriteIds) {
                Y_ABORT_UNLESS(owner.LongTxWrites.contains(writeId), "TTxInit at %" PRIu64 " : Commit %" PRIu64 " references local write %" PRIu64 " that doesn't exist",
                    owner.TabletID(), GetTxId(), (ui64)writeId);
                owner.AddLongTxWrite(writeId, GetTxId());
            }
        }

        TProposeResult Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& /*txc*/, bool /*proposed*/) const override {
            if (WriteIds.empty()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                                        TStringBuilder() << "Commit TxId# " << GetTxId() << " has an empty list of write ids");
            }

            for (auto&& writeId : WriteIds) {
                if (!owner.LongTxWrites.contains(writeId)) {
                    return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                        TStringBuilder() << "Commit TxId# " << GetTxId() << " references WriteId# " << (ui64)writeId << " that no longer exists");
                }
                auto& lw = owner.LongTxWrites[writeId];
                if (lw.PreparedTxId != 0) {
                    return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                            TStringBuilder() << "Commit TxId# " << GetTxId() << " references WriteId# " << (ui64)writeId << " that is already locked by TxId# " << lw.PreparedTxId);
                }
            }

            for (auto&& writeId : WriteIds) {
                owner.AddLongTxWrite(writeId, GetTxId());
            }
            return TProposeResult();;
        }

        bool Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
            TBlobGroupSelector dsGroupSelector(owner.Info());
            NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

            auto pathExists = [&](ui64 pathId) {
                return owner.TablesManager.HasTable(pathId);
            };

            auto counters = owner.InsertTable->Commit(dbTable, version.GetPlanStep(), version.GetTxId(), WriteIds,
                                                        pathExists);

            owner.IncCounter(COUNTER_BLOBS_COMMITTED, counters.Rows);
            owner.IncCounter(COUNTER_BYTES_COMMITTED, counters.Bytes);
            owner.IncCounter(COUNTER_RAW_BYTES_COMMITTED, counters.RawBytes);

            NIceDb::TNiceDb db(txc.DB);
            for (TWriteId writeId : WriteIds) {
                owner.RemoveLongTxWrite(db, writeId, GetTxId());
            }
            owner.UpdateInsertTableCounters();
            return true;
        }

        bool Complete(TColumnShard& owner, const TActorContext& ctx) override {
            auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
                owner.TabletID(), TxInfo.TxKind, GetTxId(), NKikimrTxColumnShard::SUCCESS);
                result->Record.SetStep(TxInfo.PlanStep);
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
            return true;
        }

        bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            NIceDb::TNiceDb db(txc.DB);
            for (TWriteId writeId : WriteIds) {
                owner.RemoveLongTxWrite(db, writeId, GetTxId());
            }
            TBlobGroupSelector dsGroupSelector(owner.Info());
            NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
            owner.InsertTable->Abort(dbTable, WriteIds);
            return true;
        }

    private:
        THashSet<TWriteId> WriteIds;
    };
}
