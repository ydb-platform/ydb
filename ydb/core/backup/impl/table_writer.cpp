#include "table_writer.h"

#include <ydb/core/tx/replication/service/table_writer_impl.h>

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/protos/change_exchange.pb.h>

namespace NKikimr::NBackup::NImpl {

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;

public:
    using TPtr = TIntrusivePtr<TChangeRecord>;
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

    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record) const {
        record.SetSourceOffset(GetOrder());
        // TODO: fill WriteTxId

        record.SetKey(ProtoBody.GetCdcDataChange().GetKey().GetData());

        auto& upsert = *record.MutableUpsert();
        *upsert.MutableTags() = {
            ProtoBody.GetCdcDataChange().GetUpsert().GetTags().begin(),
            ProtoBody.GetCdcDataChange().GetUpsert().GetTags().end()};
        upsert.SetData(ProtoBody.GetCdcDataChange().GetUpsert().GetData());
    }

    void Serialize(
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record,
        TChangeRecordBuilderContextTrait<TChangeRecord>&) const
    {
        return Serialize(record);
    }

    ui64 ResolvePartitionId(NChangeExchange::IChangeSenderResolver* const resolver) const override {
        const auto& partitions = resolver->GetPartitions();
        Y_ABORT_UNLESS(partitions);
        const auto& schema = resolver->GetSchema();
        const auto streamFormat = resolver->GetStreamFormat();
        Y_ABORT_UNLESS(streamFormat == NKikimrSchemeOp::ECdcStreamFormatProto);

        const auto range = TTableRange(GetKey());
        Y_ABORT_UNLESS(range.Point);

        const auto it = LowerBound(
            partitions.cbegin(), partitions.cend(), true,
            [&](const auto& partition, bool) {
                const int compares = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, schema
                );

                return (compares < 0);
            }
        );

        Y_ABORT_UNLESS(it != partitions.end());
        return it->ShardId;
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
    : public TBaseChangeRecordContainer
{
    TChangeRecordContainer() = default;

    explicit TChangeRecordContainer(TVector<NBackup::NImpl::TChangeRecord::TPtr>&& records)
        : Records(std::move(records))
    {}

    TVector<NBackup::NImpl::TChangeRecord::TPtr> Records;

    TString Out() override {
        return TStringBuilder() << "[" << JoinSeq(",", Records) << "]";
    }
};

template <>
struct TChangeRecordBuilderTrait<NBackup::NImpl::TChangeRecord>
    : public NBackup::NImpl::TChangeRecordBuilder
{};

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NBackup::NImpl::TChangeRecord, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NBackup::NImpl::TChangeRecord::TPtr, out, value) {
    return value->Out(out);
}

namespace NKikimr::NBackup::NImpl {

IActor* CreateLocalTableWriter(const TPathId& tablePathId) {
    return new NReplication::NService::TLocalTableWriter<NBackup::NImpl::TChangeRecord>(tablePathId);
}

}
