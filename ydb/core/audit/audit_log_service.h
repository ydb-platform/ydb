#pragma once

#include <util/generic/fwd.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/protos/audit.pb.h>
#include <ydb/core/protos/config.pb.h>

#include <memory>

class TLogBackend;

namespace NActors {
    class IActor;
}

namespace NKikimr::NAudit {

inline NActors::TActorId MakeAuditServiceID() {
    return NActors::TActorId(0, TStringBuf("YDB_AUDIT"));
}

inline NActors::TActorId MakeTopicCloudEventsAuditServiceID() {
    return NActors::TActorId(0, TStringBuf("YDB_TCE_AUD"));  // Topic Cloud Events Audit; MaxServiceIDLength=12
}

using TAuditLogBackends = TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>>;

std::unique_ptr<NActors::IActor> CreateAuditWriter(TAuditLogBackends&& logBackends);

}   // namespace NKikimr::NAudit
