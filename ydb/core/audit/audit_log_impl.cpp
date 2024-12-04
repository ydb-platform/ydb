#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/logger/backend.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include "audit_events.h"
#include "audit_log_service.h"
#include "audit_log.h"

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_E
# error log macro redefinition
#endif

#define LOG_T(stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::AUDIT_LOG_WRITER, stream)
#define LOG_D(stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::AUDIT_LOG_WRITER, stream)
#define LOG_I(stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::AUDIT_LOG_WRITER, stream)
#define LOG_N(stream) LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::AUDIT_LOG_WRITER, stream)
#define LOG_W(stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::AUDIT_LOG_WRITER, stream)
#define LOG_E(stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::AUDIT_LOG_WRITER, stream)

namespace NKikimr::NAudit {

void WriteLog(const TString& log, const TVector<THolder<TLogBackend>>& logBackends) {
    for (auto& logBackend : logBackends) {
        try {
            logBackend->WriteData(TLogRecord(
                ELogPriority::TLOG_INFO,
                log.data(),
                log.length()
            ));
        } catch (const yexception& e) {
            LOG_E("WriteLog: unable to write audit log (error: " << e.what() << ")");
        }
    }
}

TString GetJsonLog(const NEvAuditLog::TEvWriteAuditLog* ev) {
    TStringStream ss;
    ss << ev->Time << ": ";
    NJson::TJsonMap m;
    for (auto& [k, v] : ev->Parts) {
        m[k] = v;
    }
    NJson::WriteJson(&ss, &m, false, false);
    ss << Endl;
    return ss.Str();
}

TString GetJsonLogCompatibleLog(const NEvAuditLog::TEvWriteAuditLog* ev) {
    TStringStream ss;
    NJsonWriter::TBuf json(NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss);
    {
        auto obj = json.BeginObject();
        obj
            .WriteKey("@timestamp")
            .WriteString(ev->Time.ToString().data())
            .WriteKey("@log_type")
            .WriteString("audit");

        for (auto& [k, v] : ev->Parts) {
            obj.WriteKey(k).WriteString(v);
        }
        json.EndObject();
    }
    ss << Endl;
    return ss.Str();
}

TString GetTxtLog(const NEvAuditLog::TEvWriteAuditLog* ev) {
    TStringStream ss;
    ss << ev->Time << ": ";
    for (auto it = ev->Parts.begin(); it != ev->Parts.end(); it++) {
        if (it != ev->Parts.begin())
            ss << ", ";
        ss << it->first << "=" << it->second;
    }
    ss << Endl;
    return ss.Str();
}

// Array of functions for converting NEvAuditLog::TEvWriteAuditLog events to a string.
// Indexing in the array occurs by the value of the NKikimrConfig::TAuditConfig::EFormat enumeration.
// The size of AuditLogItemBuilders must be equal to the maximum value of the NKikimrConfig::TAuditConfig::EFormat enumeration.
static std::vector<TAuditLogItemBuilder> AuditLogItemBuilders = { GetJsonLog, GetTxtLog, GetJsonLogCompatibleLog, nullptr };

// numbering enumeration starts from one
static constexpr size_t DefaultAuditLogItemBuilder = static_cast<size_t>(NKikimrConfig::TAuditConfig::JSON) - 1;

void RegisterAuditLogItemBuilder(NKikimrConfig::TAuditConfig::EFormat format, TAuditLogItemBuilder builder) {
    size_t index = static_cast<size_t>(format);
    if (index < AuditLogItemBuilders.size()) {
        AuditLogItemBuilders[index] = builder;
    }
}

class TAuditLogActor final : public TActor<TAuditLogActor> {
private:
    const TAuditLogBackends LogBackends;

public:
    TAuditLogActor(TAuditLogBackends&& logBackends)
        : TActor(&TThis::StateWork)
        , LogBackends(std::move(logBackends))
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::AUDIT_WRITER_ACTOR;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(NEvAuditLog::TEvWriteAuditLog, HandleWriteAuditLog);
        default:
            HandleUnexpectedEvent(ev);
            break;
        }
    }

    void HandlePoisonPill(const TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        AUDIT_LOG_ENABLED.store(false);
        Die(ctx);
    }

    void HandleWriteAuditLog(const NEvAuditLog::TEvWriteAuditLog::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        for (auto& logBackends : LogBackends) {
            const auto builderIndex = static_cast<size_t>(logBackends.first) - 1;
            const auto builder = builderIndex < AuditLogItemBuilders.size()
                ? AuditLogItemBuilders[builderIndex] : AuditLogItemBuilders[DefaultAuditLogItemBuilder];
            const auto auditLogItem = builder(ev->Get());
            WriteLog(auditLogItem, logBackends.second);
        }
    }

    void HandleUnexpectedEvent(STFUNC_SIG) {
        LOG_W("TAuditLogActor:"
            << " unhandled event type: " << ev->GetTypeRewrite()
            << " event: " << ev->GetTypeName()
        );
    }
};

// Client interface implementation
//

std::atomic<bool> AUDIT_LOG_ENABLED = false;

void SendAuditLog(const NActors::TActorSystem* sys, TVector<std::pair<TString, TString>>&& parts)
{
    auto request = MakeHolder<NEvAuditLog::TEvWriteAuditLog>(Now(), std::move(parts));
    sys->Send(MakeAuditServiceID(), request.Release());
}

// Service interface implementation
//

THolder<NActors::IActor> CreateAuditWriter(TAuditLogBackends&& logBackends)
{
    AUDIT_LOG_ENABLED.store(true);
    return MakeHolder<TAuditLogActor>(std::move(logBackends));
}

}    // namespace NKikimr::NAudit
