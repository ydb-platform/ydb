#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/logger/backend.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/logger/record.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/strbuf.h>
#include <util/datetime/base.h>

#define AUDIT_LOG_S(sys, expr)                                                                                                  \
    do {                                                                                                                        \
        if (::NKikimr::NAudit::AUDIT_LOG_ENABLED.load()) {                                                                      \
            TVector<std::pair<TString, TString>> auditParts;                                                                    \
            expr                                                                                                                \
            ::NKikimr::NAudit::SendAuditLog(sys, std::move(auditParts));                                                        \
        }                                                                                                                       \
    } while (0) /**/

#define AUDIT_LOG(expr) AUDIT_LOG_S((TlsActivationContext->ExecutorThread.ActorSystem), expr)

#define AUDIT_PART_NO_COND(key, value) AUDIT_PART_COND(key, value, true)
#define AUDIT_PART_COND(key, value, condition)                                                                                    \
    do {                                                                                                                          \
        if (condition && !value.empty()) {                                                                                        \
            auditParts.emplace_back(key, value);                                                                                  \
        }                                                                                                                         \
    } while (0);

#define GET_AUDIT_PART_MACRO(_1, _2, _3, NAME,...) NAME
#define AUDIT_PART(...) GET_AUDIT_PART_MACRO(__VA_ARGS__, AUDIT_PART_COND, AUDIT_PART_NO_COND)(__VA_ARGS__)

namespace NKikimr::NAudit {

extern std::atomic<bool> AUDIT_LOG_ENABLED;

struct TEvAuditLog
{
    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_YDB_AUDIT_LOG),

        // Request actors
        EvWriteAuditLog = EvBegin + 0,

        EvEnd
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_AUDIT_LOG),
                  "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_AUDIT_LOG)");

    struct TEvWriteAuditLog
        : public NActors::TEventLocal<TEvWriteAuditLog, EvWriteAuditLog>
    {
        TInstant Time;
        TVector<std::pair<TString, TString>> Parts;

        TEvWriteAuditLog(TInstant time, TVector<std::pair<TString, TString>>&& parts)
            : Time(time)
            , Parts(std::move(parts))
        {}
    };
};

class TAuditLogActor final
    : public TActor<TAuditLogActor>
{
private:
    const TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> LogBackends;
public:
    TAuditLogActor(TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> logBackends)
        : TActor(&TThis::StateWork)
        , LogBackends(std::move(logBackends))
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::AUDIT_WRITER_ACTOR;
    }

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteAuditLog(
        const TEvAuditLog::TEvWriteAuditLog::TPtr& ev,
        const TActorContext& ctx);

    static void WriteLog(
        const TString& log,
        const TVector<THolder<TLogBackend>>& logBackends);

    static TString GetJsonLog(
        const TEvAuditLog::TEvWriteAuditLog::TPtr& ev);

    static TString GetTxtLog(
        const TEvAuditLog::TEvWriteAuditLog::TPtr& ev);

    void HandleUnexpectedEvent(STFUNC_SIG);
};

////////////////////////////////////////////////////////////////////////////////

void SendAuditLog(const NActors::TActorSystem* sys, TVector<std::pair<TString, TString>>&& parts);

inline NActors::TActorId MakeAuditServiceID() {
    return NActors::TActorId(0, TStringBuf("YDB_AUDIT"));
}

THolder<NActors::IActor> CreateAuditWriter(TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> logBackends);

}   // namespace NKikimr::NAudit
