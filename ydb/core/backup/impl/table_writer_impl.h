#pragma once

#include "table_writer.h"

#include <ydb/core/tx/replication/service/table_writer_impl.h>

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/protos/change_exchange.pb.h>

namespace NKikimr::NBackup::NImpl {

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;

public:
    using TPtr = TIntrusivePtr<TChangeRecord>;
    using TBuilder = TChangeRecordBuilder;

    struct TSerializationContext {
        const NBackup::NImpl::EWriterType Type;

        TSerializationContext(NBackup::NImpl::EWriterType type)
            : Type(type)
        {}

        // just copy type
        TSerializationContext(const TSerializationContext& other) = default;
    };

    const static NKikimrSchemeOp::ECdcStreamFormat StreamType = NKikimrSchemeOp::ECdcStreamFormatProto;

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
    TString GetSourceId() const {
        return SourceId;
    }

    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record, TSerializationContext& ctx) const {
        switch (ctx.Type) {
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
private:
    TString SourceId;
    NKikimrChangeExchange::TChangeRecord ProtoBody;
    NReplication::NService::TLightweightSchema::TCPtr Schema;

    mutable TMaybe<TSerializedCellVec> Key;

    void SerializeBackup(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record) const {
        record.SetSourceOffset(GetOrder());
        // TODO: fill WriteTxId

        record.SetKey(ProtoBody.GetCdcDataChange().GetKey().GetData());

        auto& upsert = *record.MutableUpsert();

        switch (ProtoBody.GetCdcDataChange().GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert: {
            *upsert.MutableTags() = {
                ProtoBody.GetCdcDataChange().GetUpsert().GetTags().begin(),
                ProtoBody.GetCdcDataChange().GetUpsert().GetTags().end()};
            auto it = Schema->ValueColumns.find("__ydb_incrBackupImpl_deleted");
            Y_ABORT_UNLESS(it != Schema->ValueColumns.end(), "Invariant violation");
            upsert.AddTags(it->second.Tag);

            TString serializedCellVec = ProtoBody.GetCdcDataChange().GetUpsert().GetData();
            Y_ABORT_UNLESS(
                TSerializedCellVec::UnsafeAppendCells({TCell::Make<bool>(false)}, serializedCellVec),
                "Invalid cell format, can't append cells");

            upsert.SetData(serializedCellVec);
            break;
        }
        case NKikimrChangeExchange::TDataChange::kErase: {
            size_t size = Schema->ValueColumns.size();
            TVector<NTable::TTag> tags;
            TVector<TCell> cells;

            tags.reserve(size);
            cells.reserve(size);

            for (const auto& [name, value] : Schema->ValueColumns) {
                tags.push_back(value.Tag);
                if (name != "__ydb_incrBackupImpl_deleted") {
                    cells.emplace_back();
                } else {
                    cells.emplace_back(TCell::Make<bool>(true));
                }
            }

            *upsert.MutableTags() = {tags.begin(), tags.end()};
            upsert.SetData(TSerializedCellVec::Serialize(cells));

            break;
        }
        case NKikimrChangeExchange::TDataChange::kReset: [[fallthrough]];
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
        case NKikimrChangeExchange::TDataChange::kUpsert: {
            auto& upsert = *record.MutableUpsert();
            *upsert.MutableTags() = {
                ProtoBody.GetCdcDataChange().GetUpsert().GetTags().begin(),
                ProtoBody.GetCdcDataChange().GetUpsert().GetTags().end()};
            upsert.SetData(ProtoBody.GetCdcDataChange().GetUpsert().GetData());
            break;
        }
        case NKikimrChangeExchange::TDataChange::kErase:
            record.MutableErase();
            break;
        case NKikimrChangeExchange::TDataChange::kReset: [[fallthrough]];
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

namespace NKikimr {

template <>
struct TChangeRecordContainer<NBackup::NImpl::TChangeRecord>
    : public TBaseChangeRecordContainer<NBackup::NImpl::TChangeRecord>
{
    using TBaseChangeRecordContainer<NBackup::NImpl::TChangeRecord>::TBaseChangeRecordContainer;
};

}
