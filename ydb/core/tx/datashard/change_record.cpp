#include "change_record.h"

#include <ydb/core/protos/change_exchange.pb.h>

#include <util/stream/str.h>

namespace NKikimr {
namespace NDataShard {

i64 TChangeRecord::GetSeqNo() const {
    Y_VERIFY(Order <= Max<i64>());
    return static_cast<i64>(Order);
}

TConstArrayRef<TCell> TChangeRecord::GetKey() const {
    if (Key) {
        return *Key;
    }

    switch (Kind) {
        case EKind::AsyncIndex:
        case EKind::CdcDataChange: {
            NKikimrChangeExchange::TChangeRecord::TDataChange parsed;
            Y_VERIFY(parsed.ParseFromArray(Body.data(), Body.size()));

            TSerializedCellVec key;
            Y_VERIFY(TSerializedCellVec::TryParse(parsed.GetKey().GetData(), key));

            Key.ConstructInPlace(key.GetCells());
            break;
        }
    }

    Y_VERIFY(Key);
    return *Key;
}

void TChangeRecord::SerializeTo(NKikimrChangeExchange::TChangeRecord& record) const {
    record.SetOrder(Order);
    record.SetGroup(Group);
    record.SetStep(Step);
    record.SetTxId(TxId);
    record.SetPathOwnerId(PathId.OwnerId);
    record.SetLocalPathId(PathId.LocalPathId);

    switch (Kind) {
        case EKind::AsyncIndex: {
            Y_VERIFY(record.MutableAsyncIndex()->ParseFromArray(Body.data(), Body.size()));
            break;
        }
        case EKind::CdcDataChange: {
            Y_VERIFY(record.MutableCdcDataChange()->ParseFromArray(Body.data(), Body.size()));
            break;
        }
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
        << " Body: " << Body.size() << "b"
    << " }";
}

TChangeRecordBuilder::TChangeRecordBuilder(EKind kind) {
    Record.Kind = kind;
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

TChangeRecordBuilder& TChangeRecordBuilder::WithBody(const TString& body) {
    Record.Body = body;
    return *this;
}

TChangeRecordBuilder& TChangeRecordBuilder::WithBody(TString&& body) {
    Record.Body = std::move(body);
    return *this;
}

TChangeRecord&& TChangeRecordBuilder::Build() {
    return std::move(Record);
}

} // NDataShard
} // NKikimr
