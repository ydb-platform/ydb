#pragma once

#include <util/generic/fwd.h>

#include <ydb/library/actors/core/actorid.h>
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

using TAuditLogBackends = TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>>;

std::unique_ptr<NActors::IActor> CreateAuditWriter(TAuditLogBackends&& logBackends);

}   // namespace NKikimr::NAudit
