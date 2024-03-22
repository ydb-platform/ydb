#pragma once

#include <util/generic/hash.h>

#include <tuple>

namespace NKikimrReplication {
    class TWorkerIdentity;
}

namespace NKikimr::NReplication {

struct TWorkerId: std::tuple<ui64, ui64, ui64> {
    explicit TWorkerId(ui64 rid, ui64 tid, ui64 wid);

    ui64 ReplicationId() const;
    ui64 TargetId() const;
    ui64 WorkerId() const;

    static TWorkerId Parse(const NKikimrReplication::TWorkerIdentity& proto);
    void Serialize(NKikimrReplication::TWorkerIdentity& proto) const;

    void Out(IOutputStream& out) const;
};

}

template <>
struct THash<NKikimr::NReplication::TWorkerId> : THash<std::tuple<ui64, ui64, ui64>> {};
