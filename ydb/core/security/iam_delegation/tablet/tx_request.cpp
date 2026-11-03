#include "tablet.h"

#include <ydb/core/base/path.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NIamDelegation {
namespace {

using namespace NKikimrIamDelegation;

enum class EReadStatus { Ready, Missing, Retry };
enum class EExecution { Complete, Retry };

bool ValidId(const TString& value) {
    return !value.empty() && value.size() <= 256;
}

bool ValidPath(const TString& value) {
    if (value.empty() || value.size() > 4096 || value.front() != '/' || CanonizePath(value) != value) {
        return false;
    }
    for (const auto& part : SplitPath(value)) {
        if (IsPathPartContainsOnlyDots(part)) {
            return false;
        }
    }
    return true;
}

template <class T>
void Parse(const TString& data, T& result) {
    Y_ABORT_UNLESS(result.ParseFromString(data), "Corrupt IAM delegation tablet record");
}

EReadStatus ReadDatabase(NIceDb::TNiceDb& db, const TString& incarnation, TDatabaseRecord& result) {
    auto row = db.Table<TSchema::Databases>().Key(incarnation).Select<TSchema::Databases::Data>();
    if (!row.IsReady()) {
        return EReadStatus::Retry;
    }
    if (row.EndOfSet()) {
        return EReadStatus::Missing;
    }
    Parse(row.GetValue<TSchema::Databases::Data>(), result);
    return EReadStatus::Ready;
}

void WriteDatabase(NIceDb::TNiceDb& db, TDatabaseRecord& record) {
    record.SetInventoryRevision(record.GetInventoryRevision() + 1);
    db.Table<TSchema::Databases>().Key(record.GetIdentity().GetIncarnation())
        .Update(NIceDb::TUpdate<TSchema::Databases::Data>(record.SerializeAsString()));
}

} // namespace

struct TIamDelegationTablet::TTxRequest final
    : NTabletFlatExecutor::TTransactionBase<TIamDelegationTablet>
{
    explicit TTxRequest(TIamDelegationTablet* self, TEvIamDelegationTablet::TEvRequest::TPtr&& event)
        : TTransactionBase(self)
        , Event(std::move(event))
    {}

    TTxType GetTxType() const override { return static_cast<TTxType>(ETxType::Request); }

    EExecution Error(EStatus status, const TString& message) {
        Response->Record.SetStatus(status);
        Response->Record.SetError(message);
        return EExecution::Complete;
    }

    EExecution RegisterDatabase(NIceDb::TNiceDb& db, const TRegisterDatabaseRequest& request) {
        const auto& identity = request.GetIdentity();
        if (!ValidId(identity.GetIncarnation()) || !ValidPath(identity.GetPath()) || identity.GetDatabaseId().size() > 256) {
            return Error(INVALID_ARGUMENT, "A bounded incarnation and absolute path are required; database ID is an optional bounded alias");
        }
        TDatabaseRecord record;
        const auto status = ReadDatabase(db, identity.GetIncarnation(), record);
        if (status == EReadStatus::Retry) {
            return EExecution::Retry;
        }
        if (status == EReadStatus::Ready) {
            if (record.GetIdentity().SerializeAsString() != identity.SerializeAsString()) {
                return Error(CONFLICT, "Database incarnation is already associated with another identity");
            }
        } else {
            *record.MutableIdentity() = identity;
            WriteDatabase(db, record);
        }
        *Response->Record.MutableDatabase() = record;
        return EExecution::Complete;
    }

    EExecution ServiceGate(NIceDb::TNiceDb& db, const TServiceKey& input,
            const TRequest& command, ui64 nowUs)
    {
        const auto kind = command.Command_case();
        const auto& check = command.GetCheckService();
        const auto& report = command.GetReportService();
        if (!ValidId(input.GetServiceId()) || !ValidId(input.GetMicroserviceId())
            || !ValidId(input.GetSystemTokenName()) || !ValidId(input.GetResourceType())
            || input.GetEndpoint().empty() || input.GetEndpoint().size() > 1024
            || !input.GetReflection()->GetUnknownFields(input).empty()
            || (kind == TRequest::kCheckService && (!check.GetRequestTimeoutUs() || check.GetRequestTimeoutUs() > 300'000'000))
            || (kind == TRequest::kReportService && (!report.GetVersion()
                || (report.GetHealth() != SERVICE_HEALTHY && report.GetHealth() != SERVICE_UNAVAILABLE
                    && report.GetHealth() != SERVICE_CREDENTIALS_REJECTED)))) {
            return Error(INVALID_ARGUMENT, "Complete service identity, bounded request timeout and known outcome are required");
        }
        // Serialize a fresh known-field key: protobuf field ordering and unknown
        // fields from a sender must not create another service retry authority.
        TServiceKey key;
        key.SetEndpoint(input.GetEndpoint());
        key.SetServiceId(input.GetServiceId());
        key.SetMicroserviceId(input.GetMicroserviceId());
        key.SetSystemTokenName(input.GetSystemTokenName());
        key.SetResourceType(input.GetResourceType());
        const auto encoded = key.SerializeAsString();
        auto row = db.Table<TSchema::ServiceBackoff>().Key(encoded).Select<TSchema::ServiceBackoff::Data>();
        if (!row.IsReady()) {
            return EExecution::Retry;
        }
        TServiceBackoffState state;
        if (row.EndOfSet()) {
            if (kind == TRequest::kReportService) {
                return Error(NOT_FOUND, "Service admission scope does not exist");
            }
            state.SetVersion(1);
            state.SetIteration(0);
            state.SetNextAllowedAtUs(0);
        } else {
            Parse(row.GetValue<TSchema::ServiceBackoff::Data>(), state);
            Y_ABORT_UNLESS(state.GetVersion() && state.GetIteration() <= 8, "Corrupt service backoff state");
        }
        auto* gate = Response->Record.MutableServiceGate();
        gate->SetServerNowUs(nowUs);
        bool changed = row.EndOfSet();
        if (kind == TRequest::kCheckService) {
            if (!check.HasExpectedVersion()) {
                gate->SetDisposition(SERVICE_OBSERVED);
            } else if (check.GetExpectedVersion() != state.GetVersion()) {
                gate->SetDisposition(SERVICE_STALE);
            } else if (nowUs < state.GetNextAllowedAtUs()) {
                gate->SetDisposition(SERVICE_WAIT);
            } else {
                gate->SetDisposition(SERVICE_ADMITTED);
                if (state.GetIteration()) {
                    // The authoritative version changes before the permission
                    // is returned; other simultaneous wakeups must recheck.
                    if (state.GetVersion() == Max<ui64>() || check.GetRequestTimeoutUs() > Max<ui64>() - nowUs) {
                        return Error(PRECONDITION_FAILED, "Service admission clock/version exhausted");
                    }
                    state.SetVersion(state.GetVersion() + 1);
                    state.SetNextAllowedAtUs(nowUs + check.GetRequestTimeoutUs());
                    changed = true;
                }
            }
        } else if (report.GetVersion() != state.GetVersion()) {
            gate->SetDisposition(SERVICE_STALE);
        } else {
            gate->SetDisposition(SERVICE_UPDATED);
            if (report.GetHealth() != SERVICE_HEALTHY || state.GetIteration()) {
                if (state.GetVersion() == Max<ui64>()) {
                    return Error(PRECONDITION_FAILED, "Service admission version exhausted");
                }
                state.SetVersion(state.GetVersion() + 1);
                changed = true;
                if (report.GetHealth() == SERVICE_HEALTHY) {
                    state.SetIteration(0);
                    state.SetNextAllowedAtUs(0);
                } else {
                    const auto iteration = Min<ui32>(state.GetIteration() + 1, 8);
                    TBackoff backoff(TDuration::Seconds(1), TDuration::Minutes(1));
                    TDuration delay;
                    for (ui32 i = 0; i < iteration; ++i) {
                        delay = backoff.Next();
                    }
                    if (delay.MicroSeconds() > Max<ui64>() - nowUs) {
                        return Error(PRECONDITION_FAILED, "Service admission clock exhausted");
                    }
                    state.SetIteration(iteration);
                    state.SetNextAllowedAtUs(nowUs + delay.MicroSeconds());
                }
            }
        }
        if (changed) {
            db.Table<TSchema::ServiceBackoff>().Key(encoded)
                .Update(NIceDb::TUpdate<TSchema::ServiceBackoff::Data>(state.SerializeAsString()));
        }
        *gate->MutableState() = std::move(state);
        return EExecution::Complete;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        // Execute may restart after a page fault. Do not keep a partial response.
        Response = MakeHolder<TEvIamDelegationTablet::TEvResponse>();
        Response->Record.SetStatus(SUCCESS);
        NIceDb::TNiceDb db(txc.DB);
        const auto& request = Event->Get()->Record;
        EExecution result = EExecution::Complete;
        switch (request.Command_case()) {
            case TRequest::kRegisterDatabase: result = RegisterDatabase(db, request.GetRegisterDatabase()); break;
            case TRequest::kCheckService:
                result = ServiceGate(db, request.GetCheckService().GetKey(), request, ctx.Now().MicroSeconds()); break;
            case TRequest::kReportService:
                result = ServiceGate(db, request.GetReportService().GetKey(), request, ctx.Now().MicroSeconds()); break;
            case TRequest::COMMAND_NOT_SET: result = Error(INVALID_ARGUMENT, "A tablet command is required"); break;
        }
        return result == EExecution::Complete;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Event->Sender, Response.Release(), 0, Event->Cookie);
    }

    TEvIamDelegationTablet::TEvRequest::TPtr Event;
    THolder<TEvIamDelegationTablet::TEvResponse> Response;
};

void TIamDelegationTablet::Handle(TEvIamDelegationTablet::TEvRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxRequest(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NIamDelegation
