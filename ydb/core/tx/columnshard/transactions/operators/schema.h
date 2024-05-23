#pragma once

#include "propose_tx.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

    class TSchemaTransactionOperator : public IProposeTxOperator {
    private:
        using TBase = IProposeTxOperator;

        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TSchemaTransactionOperator>(NKikimrTxColumnShard::TX_KIND_SCHEMA);

        virtual TTxController::TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
        virtual void DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
            AFL_VERIFY(false)("error", "method not implemented for non-async operator by default");
        }
        virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
            AFL_VERIFY(false)("error", "method not implemented for non-async operator by default");
        }
        virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
        }
        virtual bool DoIsAsync() const override {
            return false;
        }
        virtual bool DoParse(TColumnShard& /*owner*/, const TString& data) override {
            return SchemaTxBody.ParseFromString(data);
        }
    public:
        using TBase::TBase;

        bool TxWithDeadline() const override {
            return false;
        }

        virtual bool ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
            owner.RunSchemaTx(SchemaTxBody, version, txc);
            owner.ProtectSchemaSeqNo(SchemaTxBody.GetSeqNo(), txc);
            return true;
        }

        virtual bool CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) override {
            for (TActorId subscriber : NotifySubscribers) {
                auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(owner.TabletID(), GetTxId());
                ctx.Send(subscriber, event.Release(), 0, 0);
            }

            auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
                owner.TabletID(), TxInfo.TxKind, TxInfo.TxId, NKikimrTxColumnShard::SUCCESS);
            result->Record.SetStep(TxInfo.PlanStep);
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
            return true;
        }

        virtual bool ExecuteOnAbort(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
            return true;
        }
        virtual bool CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
            return true;
        }

        virtual void RegisterSubscriber(const TActorId& actorId) override {
            NotifySubscribers.insert(actorId);
        }

    private:
        TConclusionStatus ValidateTables(::google::protobuf::RepeatedPtrField<::NKikimrTxColumnShard::TCreateTable> tables) const;

        TConclusionStatus ValidateTableSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) const;

        TConclusionStatus ValidateTablePreset(const NKikimrSchemeOp::TColumnTableSchemaPreset& preset) const {
            if (preset.HasName() && preset.GetName() != "default") {
                return TConclusionStatus::Fail("Preset name must be empty or 'default', but '" + preset.GetName() + "' got");
            }
            return ValidateTableSchema(preset.GetSchema());
        }

    private:
        NKikimrTxColumnShard::TSchemaTxBody SchemaTxBody;
        THashSet<TActorId> NotifySubscribers;
    };

}
