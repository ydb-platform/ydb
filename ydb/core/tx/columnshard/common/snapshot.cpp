#include "snapshot.h"
#include <ydb/core/tx/columnshard/common/protos/snapshot.pb.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {

TString TSnapshot::DebugString() const {
    return TStringBuilder() << "plan_step=" << PlanStep << ";tx_id=" << TxId << ";";
}

NKikimrColumnShardProto::TSnapshot TSnapshot::SerializeToProto() const {
    NKikimrColumnShardProto::TSnapshot result;
    SerializeToProto(result);
    return result;
}

};
