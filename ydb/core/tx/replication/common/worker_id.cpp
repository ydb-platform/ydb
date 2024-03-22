#include "worker_id.h"

#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication {

TWorkerId::TWorkerId(ui64 rid, ui64 tid, ui64 wid)
    : std::tuple<ui64, ui64, ui64>(rid, tid, wid)
{
}

ui64 TWorkerId::ReplicationId() const {
    return std::get<0>(*this);
}

ui64 TWorkerId::TargetId() const {
    return std::get<1>(*this);
}

ui64 TWorkerId::WorkerId() const {
    return std::get<2>(*this);
}

TWorkerId TWorkerId::Parse(const NKikimrReplication::TWorkerIdentity& proto) {
    return TWorkerId(proto.GetReplicationId(), proto.GetTargetId(), proto.GetWorkerId());
}

void TWorkerId::Serialize(NKikimrReplication::TWorkerIdentity& proto) const {
    proto.SetReplicationId(ReplicationId());
    proto.SetTargetId(TargetId());
    proto.SetWorkerId(WorkerId());
}

void TWorkerId::Out(IOutputStream& out) const {
    out << ReplicationId() << ":" << TargetId() << ":" << WorkerId();
}

}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::TWorkerId, o, x) {
    return x.Out(o);
}
