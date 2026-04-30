#pragma once

#include <utility>
#include <atomic>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <ydb/library/actors/struct_log/structured_message.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

#define AUDIT_LOG_S(sys, expr)                                                                                                  \
    do {                                                                                                                        \
        if (::NKikimr::NAudit::AUDIT_LOG_ENABLED.load()) {                                                                      \
            TVector<std::pair<TString, TString>> auditParts;                                                                    \
            NKikimr::NStructuredLog::TStructuredMessage auditStructMessage;                                                         \
            expr                                                                                                                \
            ::NKikimr::NAudit::SendAuditLog(sys, std::move(auditParts));                                                        \
            YDBLOG_COMP_NOTICE(AUDIT_LOG_WRITER, "Audit event", auditStructMessage);                                            \
        }                                                                                                                       \
    } while (0) /**/
#define AUDIT_LOG(expr) AUDIT_LOG_S((::NActors::TActivationContext::ActorSystem()), expr)

#define AUDIT_PART_NO_COND(key, value) AUDIT_PART_COND(key, value, true)
#define AUDIT_PART_COND(key, value, condition)                                                                                    \
    do {                                                                                                                          \
        if (condition && !TStringBuf(value).empty()) {                                                                            \
            auditParts.emplace_back(key, value);                                                                                  \
            auditStructMessage.AppendValue({key}, TString(value));                                                                \
        }                                                                                                                         \
    } while (0);

#define GET_AUDIT_PART_MACRO(_1, _2, _3, NAME,...) NAME
#define AUDIT_PART(...) GET_AUDIT_PART_MACRO(__VA_ARGS__, AUDIT_PART_COND, AUDIT_PART_NO_COND)(__VA_ARGS__)

namespace NActors {
    class TActorSystem;
}

namespace NKikimr::NAudit {

using TAuditLogParts = TVector<std::pair<TString, TString>>;

extern std::atomic<bool> AUDIT_LOG_ENABLED;

void SendAuditLog(const NActors::TActorSystem* sys, TAuditLogParts&& parts);

}   // namespace NKikimr::NAudit
