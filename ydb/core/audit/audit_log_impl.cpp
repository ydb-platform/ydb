#include <util/charset/utf8.h>
#include <util/string/hex.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/logger/backend.h>

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include "audit_log_impl.h"
#include "audit_log_item_builder.h"
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

// TAuditLogActor
//

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

void WriteLog(const TString& log, const TVector<THolder<TLogBackend>>& logBackends) {
    for (auto& logBackend : logBackends) {
        try {
            logBackend->WriteData(TLogRecord(
                ELogPriority::TLOG_INFO,
                log.data(),
                log.length()
            ));
        } catch (const std::exception& e) {
            LOG_E("WriteLog: unable to write audit log (error: " << e.what() << ")");
        }
    }
}

TString GetJsonLog(TInstant time, const TAuditLogParts& parts) {
    TStringStream ss;
    ss << time << ": ";
    NJson::TJsonMap m;
    for (auto& [k, v] : parts) {
        m[k] = v;
    }
    NJson::WriteJson(&ss, &m, false, false);
    ss << Endl;
    return ss.Str();
}

TString GetJsonLogCompatibleLog(TInstant time, const TAuditLogParts& parts) {
    TStringStream ss;
    NJsonWriter::TBuf json(NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss);
    {
        auto obj = json.BeginObject();
        obj
            .WriteKey("@timestamp")
            .WriteString(time.ToString().data())
            .WriteKey("@log_type")
            .WriteString("audit");

        for (auto& [k, v] : parts) {
            obj.WriteKey(k).WriteString(v);
        }
        json.EndObject();
    }
    ss << Endl;
    return ss.Str();
}

TString GetTxtLog(TInstant time, const TAuditLogParts& parts) {
    TStringStream ss;
    ss << time << ": ";
    for (auto it = parts.begin(); it != parts.end(); it++) {
        if (it != parts.begin())
            ss << ", ";
        ss << it->first << "=" << it->second;
    }
    ss << Endl;
    return ss.Str();
}

// Array of functions for converting a audit event parameters to a string.
// Indexing in the array occurs by the value of the NKikimrConfig::TAuditConfig::EFormat enumeration.
// For each new format, we need to register the audit event conversion function.
// The size of AuditLogItemBuilders must be larger by one of the maximum value of the NKikimrConfig::TAuditConfig::EFormat enumeration.
// The first value of AuditLogItemBuilders is a stub for the convenience of indexing by enumeration value.
static std::vector<TAuditLogItemBuilder> AuditLogItemBuilders = { nullptr, GetJsonLog, GetTxtLog, GetJsonLogCompatibleLog, nullptr, nullptr };

// numbering enumeration starts from one
static constexpr size_t DefaultAuditLogItemBuilder = static_cast<size_t>(NKikimrConfig::TAuditConfig::JSON);

void RegisterAuditLogItemBuilder(NKikimrConfig::TAuditConfig::EFormat format, TAuditLogItemBuilder builder) {
    size_t index = static_cast<size_t>(format);
    if (index < AuditLogItemBuilders.size()) {
        AuditLogItemBuilders[index] = builder;
    }
}

class TAuditLogActor final : public TActor<TAuditLogActor> {
private:
    const TAuditLogBackends LogBackends;

    static inline const std::unordered_map<TString, ui32> FieldsOrder =
    {
        // operation's kind
        {"component", 0},

        // subject
        {"subject", 1},
        {"remote_address", 2},
        {"sanitized_token", 3},
        {"masked_token", 4},

        // verb
        {"operation", 5},
        {"status", 6},
        {"detailed_status", 7},
        {"reason", 8},

        // object
        // (these fields are not required for all audit logs)
        {"cloud_id", 9},
        {"folder_id", 10},
        {"resource_id", 11},
        {"database", 12}

        // specific fields
        // ...
    };

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
            hFunc(TEvAuditLog::TEvWriteAuditLog, HandleWriteAuditLog);
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

    void EscapeNonUtf8LogParts(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev) {
        NKikimr::NAudit::EscapeNonUtf8LogParts(ev->Get()->Parts);
    }

    void HandleWriteAuditLog(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev) {
        EscapeNonUtf8LogParts(ev);
        auto sortedParts = ev->Get()->Parts;

        auto cmpToSort = [](const std::pair<TString, TString>& lhs, const std::pair<TString, TString>& rhs) {
            ui32 lhsOrder, rhsOrder;

            {
                auto it = FieldsOrder.find(lhs.first);
                lhsOrder = (it == FieldsOrder.end() ? FieldsOrder.size() : it->second);
            }

            {
                auto it = FieldsOrder.find(rhs.first);
                rhsOrder = (it == FieldsOrder.end() ? FieldsOrder.size() : it->second);
            }

            return std::make_pair(lhsOrder, lhs.first) < std::make_pair(rhsOrder, rhs.first);
        };

        std::sort(sortedParts.begin(), sortedParts.end(), cmpToSort);

        for (auto& logBackends : LogBackends) {
            const auto builderIndex = static_cast<size_t>(logBackends.first);
            const auto builder = builderIndex < AuditLogItemBuilders.size() && AuditLogItemBuilders[builderIndex] != nullptr
                ? AuditLogItemBuilders[builderIndex] : AuditLogItemBuilders[DefaultAuditLogItemBuilder];
            const auto auditLogItem = builder(ev->Get()->Time, sortedParts);
            if (!auditLogItem.empty()) {
                WriteLog(auditLogItem, logBackends.second);
            }
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

void SendAuditLog(const NActors::TActorSystem* sys, TAuditLogParts&& parts)
{
    auto request = MakeHolder<TEvAuditLog::TEvWriteAuditLog>(Now(), std::move(parts));
    sys->Send(MakeAuditServiceID(), request.Release());
}

// Service interface implementation
//

std::unique_ptr<NActors::IActor> CreateAuditWriter(TAuditLogBackends&& logBackends)
{
    AUDIT_LOG_ENABLED.store(true);
    return std::make_unique<TAuditLogActor>(std::move(logBackends));
}

static void EscapeNonUtf8(TString& s) {
    if (!IsUtf(s)) {
        s = HexEncode(s);
    }
}

void EscapeNonUtf8LogParts(TAuditLogParts& parts) {
    for (auto& [k, v] : parts) {
        EscapeNonUtf8(k);
        EscapeNonUtf8(v);
    }
}

}    // namespace NKikimr::NAudit
