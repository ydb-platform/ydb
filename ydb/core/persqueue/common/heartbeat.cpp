#include "heartbeat.h"

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

THeartbeat THeartbeat::Parse(const NKikimrPQ::THeartbeat& proto) {
    return THeartbeat{
        .Version = TRowVersion(proto.GetStep(), proto.GetTxId()),
        .Data = proto.GetData(),
    };
}

void THeartbeat::Serialize(NKikimrPQ::THeartbeat& proto) const {
    proto.SetStep(Version.Step);
    proto.SetTxId(Version.TxId);
    proto.SetData(Data);
}

}
