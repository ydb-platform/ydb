#include "change_record.h"

#include <ydb/core/change_exchange/resolve_partition.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NDataShard {

static void ParseBody(google::protobuf::Message& proto, const TString& body) {
    Y_ENSURE(proto.ParseFromArray(body.data(), body.size()));
}

template <typename T>
static T ParseBody(const TString& body) {
    T proto;
    ParseBody(proto, body);
    return proto;
}

void TChangeRecord::Serialize(NKikimrChangeExchange::TChangeRecord& record) const {
    record.SetOrder(Order);
    record.SetGroup(Group);
    record.SetStep(Step);
    record.SetTxId(TxId);
    record.SetPathOwnerId(PathId.OwnerId);
    record.SetLocalPathId(PathId.LocalPathId);

    switch (Kind) {
        case EKind::AsyncIndex:
            return ParseBody(*record.MutableAsyncIndex(), Body);
        case EKind::IncrementalRestore:
            return ParseBody(*record.MutableIncrementalRestore(), Body);
        case EKind::CdcDataChange:
            return ParseBody(*record.MutableCdcDataChange(), Body);
        case EKind::CdcSchemaChange:
            return ParseBody(*record.MutableCdcSchemaChange(), Body);
        case EKind::CdcHeartbeat:
            return;
    }
}

TConstArrayRef<TCell> TChangeRecord::GetKey() const {
    if (Key) {
        return *Key;
    }

    switch (Kind) {
        case EKind::AsyncIndex:
        case EKind::IncrementalRestore:
        case EKind::CdcDataChange: {
            const auto parsed = ParseBody<NKikimrChangeExchange::TDataChange>(Body);

            TSerializedCellVec key;
            Y_ENSURE(TSerializedCellVec::TryParse(parsed.GetKey().GetData(), key));

            Key.ConstructInPlace(key.GetCells());
            break;
        }

        case EKind::CdcSchemaChange:
        case EKind::CdcHeartbeat: {
            Y_ENSURE(false, "Not supported");
        }
    }

    Y_ENSURE(Key);
    return *Key;
}

i64 TChangeRecord::GetSeqNo() const {
    Y_ENSURE(Order <= Max<i64>());
    return static_cast<i64>(Order);
}

TInstant TChangeRecord::GetApproximateCreationDateTime() const {
    return GetGroup()
        ? TInstant::MicroSeconds(GetGroup())
        : TInstant::MilliSeconds(GetStep());
}

bool TChangeRecord::IsBroadcast() const {
    switch (Kind) {
        case EKind::CdcSchemaChange:
        case EKind::CdcHeartbeat:
            return true;
        default:
            return false;
    }
}

void TChangeRecord::Accept(NChangeExchange::IVisitor& visitor) const {
    return visitor.Visit(*this);
}

void TChangeRecord::Out(IOutputStream& out) const {
    out << "{"
        << " Order: " << Order
        << " Group: " << Group
        << " Step: " << Step
        << " TxId: " << TxId
        << " PathId: " << PathId
        << " Kind: " << Kind
        << " Source: " << Source
        << " Body: " << Body.size() << "b"
        << " TableId: " << TableId
        << " SchemaVersion: " << SchemaVersion
        << " LockId: " << LockId
        << " LockOffset: " << LockOffset
    << " }";
}

class TDefaultPartitionResolver final: public NChangeExchange::TBasePartitionResolver {
public:
    TDefaultPartitionResolver(const NKikimr::TKeyDesc& keyDesc)
        : KeyDesc(keyDesc)
    {
    }

    void Visit(const TChangeRecord& record) override {
        SetPartitionId(NChangeExchange::ResolveSchemaBoundaryPartitionId(KeyDesc, record.GetKey()));
    }

private:
    const NKikimr::TKeyDesc& KeyDesc;
};

NChangeExchange::IPartitionResolverVisitor* CreateDefaultPartitionResolver(const NKikimr::TKeyDesc& keyDesc) {
    return new TDefaultPartitionResolver(keyDesc);
}

}
