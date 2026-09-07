#include "schema_change.h"

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

TSchemaChangeInfo TSchemaChangeInfo::Parse(const NKikimrPQ::TSchemaChangeInfo& proto) {
    return TSchemaChangeInfo{
        .Version = TRowVersion(proto.GetStep(), proto.GetTxId()),
        .Data = proto.GetData(),
    };
}

void TSchemaChangeInfo::Serialize(NKikimrPQ::TSchemaChangeInfo& proto) const {
    proto.SetStep(Version.Step);
    proto.SetTxId(Version.TxId);
    proto.SetData(Data);
}

}
