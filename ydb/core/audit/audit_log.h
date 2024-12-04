#pragma once

#include <utility>
#include <atomic>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/executor_thread.h>

#define AUDIT_LOG_S(sys, expr)                                                                                                  \
    do {                                                                                                                        \
        if (::NKikimr::NAudit::AUDIT_LOG_ENABLED.load()) {                                                                      \
            TVector<std::pair<TString, TString>> auditParts;                                                                    \
            expr                                                                                                                \
            ::NKikimr::NAudit::SendAuditLog(sys, std::move(auditParts));                                                        \
        }                                                                                                                       \
    } while (0) /**/

#define AUDIT_LOG(expr) AUDIT_LOG_S((::NActors::TlsActivationContext->ExecutorThread.ActorSystem), expr)

#define AUDIT_PART_NO_COND(key, value) AUDIT_PART_COND(key, value, true)
#define AUDIT_PART_COND(key, value, condition)                                                                                    \
    do {                                                                                                                          \
        if (condition && !value.empty()) {                                                                                        \
            auditParts.emplace_back(key, value);                                                                                  \
        }                                                                                                                         \
    } while (0);

#define GET_AUDIT_PART_MACRO(_1, _2, _3, NAME,...) NAME
#define AUDIT_PART(...) GET_AUDIT_PART_MACRO(__VA_ARGS__, AUDIT_PART_COND, AUDIT_PART_NO_COND)(__VA_ARGS__)

namespace NActors {
    class TActorSystem;
}

namespace NKikimr::NAudit {

extern std::atomic<bool> AUDIT_LOG_ENABLED;

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
        TVector<std::pair<TString, TString>> Parts;

        TEvWriteAuditLog(TInstant time, TVector<std::pair<TString, TString>>&& parts)
            : Time(time)
            , Parts(std::move(parts))
        {}
    };
};

using TAuditLogItemBuilder = TString(*)(const TEvAuditLog::TEvWriteAuditLog*);

void SendAuditLog(const NActors::TActorSystem* sys, TVector<std::pair<TString, TString>>&& parts);

// Registration of a function for converting audit events to a string in a specified format
void RegisterAuditLogItemBuilder(NKikimrConfig::TAuditConfig::EFormat format, TAuditLogItemBuilder builder);

}   // namespace NKikimr::NAudit
