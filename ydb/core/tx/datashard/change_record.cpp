#include "change_record.h"

#include <ydb/core/protos/change_exchange.pb.h>

namespace NKikimr::NDataShard {

void TChangeRecord::Serialize(NKikimrChangeExchange::TChangeRecord& record) const {
    record.SetOrder(Order);
    record.SetGroup(Group);
    record.SetStep(Step);
    record.SetTxId(TxId);
    record.SetPathOwnerId(PathId.OwnerId);
    record.SetLocalPathId(PathId.LocalPathId);

    switch (Kind) {
        case EKind::AsyncIndex: {
            Y_ABORT_UNLESS(record.MutableAsyncIndex()->ParseFromArray(Body.data(), Body.size()));
            break;
        }
        case EKind::CdcDataChange: {
            Y_ABORT_UNLESS(record.MutableCdcDataChange()->ParseFromArray(Body.data(), Body.size()));
            break;
        }
        case EKind::CdcHeartbeat: {
            break;
        }
    }
}

static auto ParseBody(const TString& protoBody) {
    NKikimrChangeExchange::TDataChange body;
    Y_ABORT_UNLESS(body.ParseFromArray(protoBody.data(), protoBody.size()));
    return body;
}

TConstArrayRef<TCell> TChangeRecord::GetKey() const {
    if (Key) {
        return *Key;
    }

    switch (Kind) {
        case EKind::AsyncIndex:
        case EKind::CdcDataChange: {
            const auto parsed = ParseBody(Body);

            TSerializedCellVec key;
            Y_ABORT_UNLESS(TSerializedCellVec::TryParse(parsed.GetKey().GetData(), key));

            Key.ConstructInPlace(key.GetCells());
            break;
        }

        case EKind::CdcHeartbeat: {
            Y_ABORT("Not supported");
        }
    }

    Y_ABORT_UNLESS(Key);
    return *Key;
}

i64 TChangeRecord::GetSeqNo() const {
    Y_ABORT_UNLESS(Order <= Max<i64>());
    return static_cast<i64>(Order);
}

TInstant TChangeRecord::GetApproximateCreationDateTime() const {
    return GetGroup()
        ? TInstant::FromValue(GetGroup())
        : TInstant::MilliSeconds(GetStep());
}

bool TChangeRecord::IsBroadcast() const {
    switch (Kind) {
        case EKind::CdcHeartbeat:
            return true;
        default:
            return false;
    }
}

TString TChangeRecord::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
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

TChangeRecordBuilder::TChangeRecordBuilder(EKind kind) {
    Record.Kind = kind;
}

TChangeRecordBuilder::TChangeRecordBuilder(TChangeRecord&& record) {
    Record = std::move(record);
}

TChangeRecordBuilder& TChangeRecordBuilder::WithLockId(ui64 lockId) {
    Record.LockId = lockId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithLockOffset(ui64 lockOffset) {
    Record.LockOffset = lockOffset;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithOrder(ui64 order) {
    Record.Order = order;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithGroup(ui64 group) {
    Record.Group = group;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithStep(ui64 step) {
    Record.Step = step;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithTxId(ui64 txId) {
    Record.TxId = txId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithPathId(const TPathId& pathId) {
    Record.PathId = pathId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithTableId(const TPathId& tableId) {
    Record.TableId = tableId;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithSchemaVersion(ui64 version) {
    Record.SchemaVersion = version;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithSchema(TUserTable::TCPtr schema) {
    Record.Schema = schema;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithBody(const TString& body) {
    Record.Body = body;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithBody(TString&& body) {
    Record.Body = std::move(body);
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithSource(ESource source) {
    Record.Source = source;
    return *this;
}

TChangeRecord&& TChangeRecordBuilder::Build() {
    return std::move(Record);
}

}
