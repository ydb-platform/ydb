#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

    class TSchemaTransactionOperator : public TTxController::ITransactionOperatior {
        using TBase = TTxController::ITransactionOperatior;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TSchemaTransactionOperator>(NKikimrTxColumnShard::TX_KIND_SCHEMA);
    public:
        using TBase::TBase;

        virtual bool Parse(const TString& data) override {
            if (!SchemaTxBody.ParseFromString(data)) {
                return false;
            }
            return true;
        }

        bool TxWithDeadline() const override {
            return false;
        }

        TProposeResult Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, bool /*proposed*/) const override {
            switch (SchemaTxBody.TxBody_case()) {
                case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
                    break;
                case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
                    for (auto& table : SchemaTxBody.GetEnsureTables().GetTables()) {
                        if (table.HasSchemaPreset() && !ValidateTablePreset(table.GetSchemaPreset())) {
                            return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "Invalid schema");
                        }
                        if (table.HasSchema() && !ValidateTableSchema(table.GetSchema())) {
                            return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "Invalid schema");
                        }
                    }
                    break;
                case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
                case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
                case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
                case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
                    break;
            }

            auto seqNo = SeqNoFromProto(SchemaTxBody.GetSeqNo());
            auto lastSeqNo = owner.LastSchemaSeqNo;

            // Check if proposal is outdated
            if (seqNo < lastSeqNo) {
                auto errorMessage = TStringBuilder()
                    << "Ignoring outdated schema tx proposal at tablet "
                    << owner.TabletID()
                    << " txId " << GetTxId()
                    << " ssId " << owner.CurrentSchemeShardId
                    << " seqNo " << seqNo
                    << " lastSeqNo " << lastSeqNo;
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_CHANGED, errorMessage);
            }

            owner.UpdateSchemaSeqNo(seqNo, txc);
            return TProposeResult();
        }

        virtual bool Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
            owner.RunSchemaTx(SchemaTxBody, version, txc);
            owner.ProtectSchemaSeqNo(SchemaTxBody.GetSeqNo(), txc);
            return true;
        }

        virtual bool Complete(TColumnShard& owner, const TActorContext& ctx) override {
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

        virtual bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            Y_UNUSED(owner, txc);
            return true;
        }

        virtual void RegisterSubscriber(const TActorId& actorId) override {
            NotifySubscribers.insert(actorId);
        }

    private:
        bool ValidateTableSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) const {
            namespace NTypeIds = NScheme::NTypeIds;

            static const THashSet<NScheme::TTypeId> supportedTypes = {
                NTypeIds::Timestamp,
                NTypeIds::Timestamp64,
                NTypeIds::Interval64,
                NTypeIds::Int8,
                NTypeIds::Int16,
                NTypeIds::Int32,
                NTypeIds::Int64,
                NTypeIds::Uint8,
                NTypeIds::Uint16,
                NTypeIds::Uint32,
                NTypeIds::Uint64,
                NTypeIds::Date,
                NTypeIds::Datetime,
                //NTypeIds::Interval,
                //NTypeIds::Float,
                //NTypeIds::Double,
                NTypeIds::String,
                NTypeIds::Utf8
            };

            if (!schema.HasEngine() ||
                schema.GetEngine() != NKikimrSchemeOp::EColumnTableEngine::COLUMN_ENGINE_REPLACING_TIMESERIES) {
                return false;
            }

            if (!schema.KeyColumnNamesSize()) {
                return false;
            }

            TString firstKeyColumn = schema.GetKeyColumnNames()[0];
            THashSet<TString> keyColumns(schema.GetKeyColumnNames().begin(), schema.GetKeyColumnNames().end());

            for (const NKikimrSchemeOp::TOlapColumnDescription& column : schema.GetColumns()) {
                TString name = column.GetName();
                /*
                if (column.GetNotNull() && keyColumns.contains(name)) {
                    return false;
                }
                */
                if (name == firstKeyColumn && !supportedTypes.contains(column.GetTypeId())) {
                    return false;
                }
                keyColumns.erase(name);
            }

            if (!keyColumns.empty()) {
                return false;
            }
            return true;
        }

        bool ValidateTablePreset(const NKikimrSchemeOp::TColumnTableSchemaPreset& preset) const {
            if (preset.HasName() && preset.GetName() != "default") {
                return false;
            }
            return ValidateTableSchema(preset.GetSchema());
        }

    private:
        NKikimrTxColumnShard::TSchemaTxBody SchemaTxBody;
        THashSet<TActorId> NotifySubscribers;
    };

}
