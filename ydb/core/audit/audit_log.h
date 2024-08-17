#pragma once

#include <utility>
#include <atomic>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <ydb/library/actors/core/actor.h>

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

void SendAuditLog(const NActors::TActorSystem* sys, TVector<std::pair<TString, TString>>&& parts);

}   // namespace NKikimr::NAudit
