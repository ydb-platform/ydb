#pragma once

#include "table_writer.h"

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/datashard_backup.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/replication/service/lightweight_schema.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NBackup::NImpl {

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;

public:
    ui64 GetGroup() const override {
        return ProtoBody.GetGroup();
    }
    ui64 GetStep() const override {
        return ProtoBody.GetStep();
    }
    ui64 GetTxId() const override {
        return ProtoBody.GetTxId();
    }
    EKind GetKind() const override {
        return EKind::CdcDataChange;
    }

    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record, EWriterType type) const {
        switch (type) {
            case EWriterType::Backup:
                return SerializeBackup(record);
            case EWriterType::Restore:
                return SerializeRestore(record);
        }
    }

    TConstArrayRef<TCell> GetKey() const {
        Y_ABORT_UNLESS(ProtoBody.HasCdcDataChange());
        Y_ABORT_UNLESS(ProtoBody.GetCdcDataChange().HasKey());
        Y_ABORT_UNLESS(ProtoBody.GetCdcDataChange().GetKey().HasData());
        TSerializedCellVec keyCellVec;
        Y_ABORT_UNLESS(TSerializedCellVec::TryParse(ProtoBody.GetCdcDataChange().GetKey().GetData(), keyCellVec));
        Key = keyCellVec;

        Y_ABORT_UNLESS(Key);
        return Key->GetCells();
    }

    void Accept(NChangeExchange::IVisitor& visitor) const override {
        return visitor.Visit(*this);
    }

private:
    NKikimrChangeExchange::TChangeRecord ProtoBody;
    NReplication::NService::TLightweightSchema::TCPtr Schema;

    mutable TMaybe<TSerializedCellVec> Key;

    void SerializeBackup(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record) const {
        record.SetSourceOffset(GetOrder());
        // TODO: fill WriteTxId

        record.SetKey(ProtoBody.GetCdcDataChange().GetKey().GetData());

        auto& upsert = *record.MutableUpsert();

        switch (ProtoBody.GetCdcDataChange().GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert: 
        case NKikimrChangeExchange::TDataChange::kReset: {
            TVector<NTable::TTag> tags;
            TVector<TCell> cells;
            NKikimrBackup::TChangeMetadata changeMetadata;
            
            changeMetadata.SetIsDeleted(false);

            // Handle both Upsert and Reset operations
            const bool isResetOperation = ProtoBody.GetCdcDataChange().GetRowOperationCase() == NKikimrChangeExchange::TDataChange::kReset;
            const auto& operationData = isResetOperation 
                ? ProtoBody.GetCdcDataChange().GetReset()
                : ProtoBody.GetCdcDataChange().GetUpsert();
            
            TSerializedCellVec originalCells;
            Y_ABORT_UNLESS(TSerializedCellVec::TryParse(operationData.GetData(), originalCells));
            
            tags.assign(operationData.GetTags().begin(), operationData.GetTags().end());
            cells.assign(originalCells.GetCells().begin(), originalCells.GetCells().end());
            
            THashSet<NTable::TTag> presentTags(operationData.GetTags().begin(), operationData.GetTags().end());
            for (const auto& [name, columnInfo] : Schema->ValueColumns) {
                if (name == "__ydb_incrBackupImpl_changeMetadata") {
                    continue;
                }
                
                auto* columnState = changeMetadata.AddColumnStates();
                columnState->SetTag(columnInfo.Tag);
                
                if (presentTags.contains(columnInfo.Tag)) {
                    auto it = std::find(operationData.GetTags().begin(), operationData.GetTags().end(), columnInfo.Tag);
                    if (it != operationData.GetTags().end()) {
                        size_t idx = std::distance(operationData.GetTags().begin(), it);
                        if (idx < originalCells.GetCells().size()) {
                            columnState->SetIsNull(originalCells.GetCells()[idx].IsNull());
                        } else {
                            columnState->SetIsNull(true);
                        }
                    } else {
                        columnState->SetIsNull(true);
                    }
                    columnState->SetIsChanged(true);
                } else {
                    if (isResetOperation) {
                        columnState->SetIsNull(true);
                        columnState->SetIsChanged(true);
                    } else {
                        columnState->SetIsNull(false);
                        columnState->SetIsChanged(false);
                    }
                }
            }

            auto metadataIt = Schema->ValueColumns.find("__ydb_incrBackupImpl_changeMetadata");
            Y_ABORT_UNLESS(metadataIt != Schema->ValueColumns.end(), "Invariant violation");
            tags.push_back(metadataIt->second.Tag);
            
            TString serializedMetadata;
            Y_ABORT_UNLESS(changeMetadata.SerializeToString(&serializedMetadata));
            cells.emplace_back(TCell(serializedMetadata.data(), serializedMetadata.size()));

            *upsert.MutableTags() = {tags.begin(), tags.end()};
            upsert.SetData(TSerializedCellVec::Serialize(cells));
            break;
        }
        case NKikimrChangeExchange::TDataChange::kErase: {
            size_t size = Schema->ValueColumns.size();
            TVector<NTable::TTag> tags;
            TVector<TCell> cells;
            NKikimrBackup::TChangeMetadata changeMetadata;
            
            changeMetadata.SetIsDeleted(true);

            tags.reserve(size);
            cells.reserve(size);

            for (const auto& [name, columnInfo] : Schema->ValueColumns) {
                if (name == "__ydb_incrBackupImpl_changeMetadata") {
                    continue;
                }
                
                tags.push_back(columnInfo.Tag);
                cells.emplace_back();
                
                auto* columnState = changeMetadata.AddColumnStates();
                columnState->SetTag(columnInfo.Tag);
                columnState->SetIsNull(true);
                columnState->SetIsChanged(true);
            }

            auto metadataIt = Schema->ValueColumns.find("__ydb_incrBackupImpl_changeMetadata");
            Y_ABORT_UNLESS(metadataIt != Schema->ValueColumns.end(), "Invariant violation");
            tags.push_back(metadataIt->second.Tag);
            
            TString serializedMetadata;
            Y_ABORT_UNLESS(changeMetadata.SerializeToString(&serializedMetadata));
            cells.emplace_back(TCell(serializedMetadata.data(), serializedMetadata.size()));

            *upsert.MutableTags() = {tags.begin(), tags.end()};
            upsert.SetData(TSerializedCellVec::Serialize(cells));
            break;
        }
        default:
            Y_FAIL_S("Unexpected row operation: " << static_cast<int>(ProtoBody.GetCdcDataChange().GetRowOperationCase()));
        }
    }

    // just pass through, all conversions are on level above
    void SerializeRestore(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record) const {
        record.SetSourceOffset(GetOrder());
        // TODO: fill WriteTxId

        record.SetKey(ProtoBody.GetCdcDataChange().GetKey().GetData());

        switch (ProtoBody.GetCdcDataChange().GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
        case NKikimrChangeExchange::TDataChange::kReset: {
            auto& upsert = *record.MutableUpsert();
            // Check if NewImage is available, otherwise fall back to Upsert/Reset
            if (ProtoBody.GetCdcDataChange().has_newimage()) {
                *upsert.MutableTags() = {
                    ProtoBody.GetCdcDataChange().GetNewImage().GetTags().begin(),
                    ProtoBody.GetCdcDataChange().GetNewImage().GetTags().end()};
                upsert.SetData(ProtoBody.GetCdcDataChange().GetNewImage().GetData());
            } else if (ProtoBody.GetCdcDataChange().GetRowOperationCase() == NKikimrChangeExchange::TDataChange::kUpsert) {
                // Fallback to Upsert field if NewImage is not available
                *upsert.MutableTags() = {
                    ProtoBody.GetCdcDataChange().GetUpsert().GetTags().begin(),
                    ProtoBody.GetCdcDataChange().GetUpsert().GetTags().end()};
                upsert.SetData(ProtoBody.GetCdcDataChange().GetUpsert().GetData());
            } else if (ProtoBody.GetCdcDataChange().GetRowOperationCase() == NKikimrChangeExchange::TDataChange::kReset) {
                Y_ABORT("Reset operation is not supported, all operations must be converted to Upsert");
            }
            break;
        }
        case NKikimrChangeExchange::TDataChange::kErase:
            record.MutableErase();
            break;
        default:
            Y_FAIL_S("Unexpected row operation: " << static_cast<int>(ProtoBody.GetCdcDataChange().GetRowOperationCase()));
        }
    }

}; // TChangeRecord

class TChangeRecordBuilder: public NChangeExchange::TChangeRecordBuilder<TChangeRecord, TChangeRecordBuilder> {
public:
    using TBase::TBase;

    TSelf& WithSourceId(const TString& sourceId) {
        GetRecord()->SourceId = sourceId;
        return static_cast<TSelf&>(*this);
    }

    template <typename T>
    TSelf& WithBody(T&& body) {
        Y_ABORT_UNLESS(GetRecord()->ProtoBody.ParseFromString(body));
        return static_cast<TBase*>(this)->WithBody(std::forward<T>(body));
    }

    TSelf& WithSchema(NReplication::NService::TLightweightSchema::TCPtr schema) {
        GetRecord()->Schema = schema;
        return static_cast<TSelf&>(*this);
    }

}; // TChangeRecordBuilder

}
