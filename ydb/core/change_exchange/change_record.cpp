#include "change_record.h"

#include <ydb/core/protos/change_exchange.pb.h>

namespace NKikimr::NChangeExchange {

void TChangeRecord::Serialize(NKikimrChangeExchange::TChangeRecord& record) const {
    record.SetOrder(Order);
    record.SetGroup(Group);
    record.SetStep(Step);
    record.SetTxId(TxId);

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
        ? TInstant::MicroSeconds(GetGroup())
        : TInstant::MilliSeconds(GetStep());
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
        << " Kind: " << Kind
        << " Source: " << Source
        << " Body: " << Body.size() << "b"
    << " }";
}

}
