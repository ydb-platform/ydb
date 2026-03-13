#pragma once
#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NAudit {

void EscapeNonUtf8LogParts(TAuditLogParts& parts);

struct TEvAuditLog {
    //
    // Events declaration
    //

    enum EEvents {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_YDB_AUDIT_LOG),

        // Request actors
        EvWriteAuditLog = EvBegin + 0,

        EvEnd
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_AUDIT_LOG),
        "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_AUDIT_LOG)"
    );

    struct TEvWriteAuditLog : public NActors::TEventLocal<TEvWriteAuditLog, EvWriteAuditLog> {
        TInstant Time;
        TAuditLogParts Parts;

        TEvWriteAuditLog(TInstant time, TAuditLogParts&& parts)
            : Time(time)
            , Parts(std::move(parts))
        {}
    };
};

auto CreateAuditLogRequest(TAuditLogParts&& parts);

} // namespace NKikimr::NAudit
