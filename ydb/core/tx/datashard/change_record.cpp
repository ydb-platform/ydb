#include "change_record.h"

#include <ydb/core/protos/change_exchange.pb.h>

namespace NKikimr::NDataShard {

void TChangeRecord::Serialize(NKikimrChangeExchange::TChangeRecord& record) const {
    NChangeExchange::TChangeRecord::Serialize(record);
    record.SetPathOwnerId(PathId.OwnerId);
    record.SetLocalPathId(PathId.LocalPathId);
}

bool TChangeRecord::IsBroadcast() const {
    switch (Kind) {
        case EKind::CdcHeartbeat:
            return true;
        default:
            return false;
    }
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

}
