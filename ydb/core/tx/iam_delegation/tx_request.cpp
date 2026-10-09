#include "tablet.h"

#include <ydb/core/base/path.h>

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

bool ValidSecret(const TSecretIdentity& identity) {
    return ValidId(identity.GetDatabaseIncarnation()) && identity.GetPathOwnerId() && identity.GetPathLocalId();
}

bool ValidBinding(const TIamBinding& binding) {
    return ValidId(binding.GetServiceAccountId()) && ValidId(binding.GetCloudId())
        && ValidId(binding.GetResourceType()) && ValidId(binding.GetReferrerId())
        && ValidId(binding.GetReferrerType()) && ValidId(binding.GetServiceId())
        && ValidId(binding.GetMicroserviceId())
        && (binding.GetReferencePolicy() == WITHOUT_REFERENCES || binding.GetReferencePolicy() == WITH_REFERENCES);
}

bool SameSecret(const TSecretIdentity& lhs, const TSecretIdentity& rhs) {
    return lhs.GetDatabaseIncarnation() == rhs.GetDatabaseIncarnation()
        && lhs.GetPathOwnerId() == rhs.GetPathOwnerId()
        && lhs.GetPathLocalId() == rhs.GetPathLocalId();
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

EReadStatus ReadSecret(NIceDb::TNiceDb& db, const TSecretIdentity& identity, TSecretRecord& result) {
    auto row = db.Table<TSchema::Secrets>()
        .Key(identity.GetDatabaseIncarnation(), identity.GetPathOwnerId(), identity.GetPathLocalId())
        .Select<TSchema::Secrets::Data>();
    if (!row.IsReady()) {
        return EReadStatus::Retry;
    }
    if (row.EndOfSet()) {
        return EReadStatus::Missing;
    }
    Parse(row.GetValue<TSchema::Secrets::Data>(), result);
    return EReadStatus::Ready;
}

EReadStatus ReadDelegation(NIceDb::TNiceDb& db, const TString& operationId, TDelegation& result,
    TString* originalStage = nullptr)
{
    auto row = db.Table<TSchema::Delegations>().Key(operationId)
        .Select<TSchema::Delegations::Data, TSchema::Delegations::OriginalStage>();
    if (!row.IsReady()) {
        return EReadStatus::Retry;
    }
    if (row.EndOfSet()) {
        return EReadStatus::Missing;
    }
    Parse(row.GetValue<TSchema::Delegations::Data>(), result);
    if (originalStage) {
        *originalStage = row.GetValue<TSchema::Delegations::OriginalStage>();
    }
    return EReadStatus::Ready;
}

void WriteDatabase(NIceDb::TNiceDb& db, TDatabaseRecord& record) {
    record.SetInventoryRevision(record.GetInventoryRevision() + 1);
    db.Table<TSchema::Databases>().Key(record.GetIdentity().GetIncarnation())
        .Update(NIceDb::TUpdate<TSchema::Databases::Data>(record.SerializeAsString()));
}

void WriteSecret(NIceDb::TNiceDb& db, const TSecretRecord& record) {
    const auto& identity = record.GetIdentity();
    db.Table<TSchema::Secrets>()
        .Key(identity.GetDatabaseIncarnation(), identity.GetPathOwnerId(), identity.GetPathLocalId())
        .Update(NIceDb::TUpdate<TSchema::Secrets::Data>(record.SerializeAsString()));
}

void WriteDelegation(NIceDb::TNiceDb& db, const TDelegation& record) {
    const TString data = record.SerializeAsString();
    db.Table<TSchema::Delegations>().Key(record.GetOperationId())
        .Update(NIceDb::TUpdate<TSchema::Delegations::Data>(data));
    if (record.GetState() == CANCELLED) {
        db.Table<TSchema::Inventory>().Key(record.GetDatabaseIncarnation(), record.GetOperationId()).Delete();
    } else {
        db.Table<TSchema::Inventory>().Key(record.GetDatabaseIncarnation(), record.GetOperationId())
            .Update(NIceDb::TUpdate<TSchema::Inventory::Data>(data));
    }
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

    EExecution ReadError(EReadStatus status, const TString& name) {
        return status == EReadStatus::Retry
            ? EExecution::Retry
            : Error(NOT_FOUND, name + " not found");
    }

    EExecution Reply(const TDelegation& delegation, const TSecretRecord* secret = nullptr, const TDatabaseRecord* database = nullptr) {
        *Response->Record.MutableDelegation() = delegation;
        if (secret) {
            *Response->Record.MutableSecret() = *secret;
        }
        if (database) {
            *Response->Record.MutableDatabase() = *database;
        }
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

    EExecution Stage(NIceDb::TNiceDb& db, const TStageRequest& request) {
        if (!ValidId(request.GetOperationId()) || !ValidId(request.GetDatabaseIncarnation())
            || !ValidPath(request.GetSecretPath()) || !ValidBinding(request.GetBinding())
            || request.GetMode() != CREATE) {
            return Error(INVALID_ARGUMENT, "A complete bounded intent and explicit IAM binding are required");
        }
        TDelegation record;
        TString originalStage;
        const auto existing = ReadDelegation(db, request.GetOperationId(), record, &originalStage);
        if (existing == EReadStatus::Retry) {
            return EExecution::Retry;
        }
        if (existing == EReadStatus::Ready) {
            if (originalStage != request.SerializeAsString()) {
                return Error(CONFLICT, "Operation ID was already used for a different intent");
            }
            TSecretRecord secret;
            if (record.HasSecret()) {
                if (const auto status = ReadSecret(db, record.GetSecret(), secret); status != EReadStatus::Ready) {
                    return ReadError(status, "Secret");
                }
            }
            return Reply(record, record.HasSecret() ? &secret : nullptr);
        }

        TDatabaseRecord database;
        if (const auto status = ReadDatabase(db, request.GetDatabaseIncarnation(), database); status != EReadStatus::Ready) {
            return ReadError(status, "Database incarnation");
        }
        const auto& databasePath = database.GetIdentity().GetPath();
        const auto& secretPath = request.GetSecretPath();
        if (secretPath.size() <= databasePath.size() || !TStringBuf(secretPath).StartsWith(databasePath)
            || secretPath[databasePath.size()] != '/') {
            return Error(INVALID_ARGUMENT, "Secret path must be a strict descendant of its registered database");
        }
        auto referrer = db.Table<TSchema::Referrers>().Key(request.GetBinding().GetReferrerId())
            .Select<TSchema::Referrers::OperationId>();
        if (!referrer.IsReady()) {
            return EExecution::Retry;
        }
        if (!referrer.EndOfSet()) {
            return Error(CONFLICT, "Referrer ID cannot be reused");
        }

        TSecretRecord secret;
        if (request.GetMode() == CREATE) {
            if (request.HasSecret() || !request.GetCreateTxId() || request.GetExpectedSecretRevision()) {
                return Error(INVALID_ARGUMENT, "CREATE requires an unbound intent and a schema transaction ID");
            }
            auto name = db.Table<TSchema::Names>().Key(request.GetDatabaseIncarnation(), request.GetSecretPath())
                .Select<TSchema::Names::OperationId>();
            if (!name.IsReady()) {
                return EExecution::Retry;
            }
            if (!name.EndOfSet()) {
                return Error(CONFLICT, "Secret name already has a live creating intent");
            }
        }

        record.SetOperationId(request.GetOperationId());
        record.SetDatabaseIncarnation(request.GetDatabaseIncarnation());
        record.SetSecretPath(request.GetSecretPath());
        record.SetCreateTxId(request.GetCreateTxId());
        *record.MutableBinding() = request.GetBinding();
        record.SetRevision(1);
        record.SetSetupState(SETUP_NOT_STARTED);
        record.SetState(PENDING);
        WriteDelegation(db, record);
        db.Table<TSchema::Delegations>().Key(record.GetOperationId())
            .Update(NIceDb::TUpdate<TSchema::Delegations::OriginalStage>(request.SerializeAsString()));
        db.Table<TSchema::Referrers>().Key(record.GetBinding().GetReferrerId())
            .Update(NIceDb::TUpdate<TSchema::Referrers::OperationId>(record.GetOperationId()));
        if (request.GetMode() == CREATE) {
            db.Table<TSchema::Names>().Key(record.GetDatabaseIncarnation(), record.GetSecretPath())
                .Update(NIceDb::TUpdate<TSchema::Names::OperationId>(record.GetOperationId()));
        }
        WriteDatabase(db, database);
        return Reply(record, record.HasSecret() ? &secret : nullptr, &database);
    }

    EExecution BindSecret(NIceDb::TNiceDb& db, const TBindSecretRequest& request) {
        if (!ValidId(request.GetOperationId()) || !ValidSecret(request.GetSecret())) {
            return Error(INVALID_ARGUMENT, "Operation and immutable secret identity are required");
        }
        TDelegation record;
        if (const auto status = ReadDelegation(db, request.GetOperationId(), record); status != EReadStatus::Ready) {
            return ReadError(status, "Delegation");
        }
        if (record.GetDatabaseIncarnation() != request.GetSecret().GetDatabaseIncarnation()) {
            return Error(CONFLICT, "Cannot bind an intent to a different database incarnation");
        }
        TSecretRecord secret;
        if (record.HasSecret()) {
            if (!SameSecret(record.GetSecret(), request.GetSecret())) {
                return Error(CONFLICT, "Intent is already bound to another immutable object");
            }
            if (const auto status = ReadSecret(db, record.GetSecret(), secret); status != EReadStatus::Ready) {
                return ReadError(status, "Secret");
            }
            return Reply(record, &secret);
        }
        if (record.GetRevision() != request.GetExpectedRevision() || record.GetState() != PENDING
            || record.GetSetupState() != SETUP_NOT_STARTED) {
            return Error(PRECONDITION_FAILED, "Only the current unbound intent can be bound");
        }
        const auto existing = ReadSecret(db, request.GetSecret(), secret);
        if (existing == EReadStatus::Retry) {
            return EExecution::Retry;
        }
        if (existing == EReadStatus::Ready) {
            return Error(CONFLICT, "Immutable object identity already belongs to another creating intent");
        }
        TDatabaseRecord database;
        if (const auto status = ReadDatabase(db, record.GetDatabaseIncarnation(), database); status != EReadStatus::Ready) {
            return ReadError(status, "Database");
        }
        *secret.MutableIdentity() = request.GetSecret();
        secret.SetSecretPath(record.GetSecretPath());
        secret.SetCreateTxId(record.GetCreateTxId());
        secret.SetCreateOperationId(record.GetOperationId());
        secret.SetPendingOperationId(record.GetOperationId());
        secret.SetRevision(1);
        secret.SetState(SECRET_LIVE);
        *record.MutableSecret() = request.GetSecret();
        record.SetRevision(record.GetRevision() + 1);
        WriteSecret(db, secret);
        WriteDelegation(db, record);
        WriteDatabase(db, database);
        return Reply(record, &secret, &database);
    }

    EExecution StartSetup(NIceDb::TNiceDb& db, const TStartSetupRequest& request) {
        TDelegation record;
        if (const auto status = ReadDelegation(db, request.GetOperationId(), record); status != EReadStatus::Ready) {
            return ReadError(status, "Delegation");
        }
        if (!record.HasSecret()) {
            return Error(PRECONDITION_FAILED, "Bind and verify the schema object before starting IAM setup");
        }
        TSecretRecord secret;
        if (const auto status = ReadSecret(db, record.GetSecret(), secret); status != EReadStatus::Ready) {
            return ReadError(status, "Secret");
        }
        if (record.GetSetupState() != SETUP_NOT_STARTED) {
            return Error(PRECONDITION_FAILED, "Setup has already started; resolve its outcome before further action");
        }
        if (record.GetRevision() != request.GetExpectedRevision() || record.GetState() != PENDING
            || secret.GetState() != SECRET_LIVE || secret.GetPendingOperationId() != record.GetOperationId()) {
            return Error(PRECONDITION_FAILED, "Setup requires the current live pending intent");
        }
        TDatabaseRecord database;
        if (const auto status = ReadDatabase(db, record.GetDatabaseIncarnation(), database); status != EReadStatus::Ready) {
            return ReadError(status, "Database");
        }
        record.SetSetupState(SETUP_STARTED);
        record.SetRevision(record.GetRevision() + 1);
        WriteDelegation(db, record);
        WriteDatabase(db, database);
        return Reply(record, &secret, &database);
    }

    EExecution SetSetupResult(NIceDb::TNiceDb& db, const TSetSetupResultRequest& request) {
        if (request.GetOutcome() != SETUP_UNKNOWN && request.GetOutcome() != SETUP_SUCCEEDED && request.GetOutcome() != SETUP_FAILED) {
            return Error(INVALID_ARGUMENT, "Setup outcome must be UNKNOWN, SUCCEEDED or FAILED");
        }
        if (request.GetSetupOperationId().size() > 256) {
            return Error(INVALID_ARGUMENT, "Setup operation ID exceeds the accepted bound");
        }
        TDelegation record;
        if (const auto status = ReadDelegation(db, request.GetOperationId(), record); status != EReadStatus::Ready) {
            return ReadError(status, "Delegation");
        }
        if (!record.HasSecret() || record.GetSetupState() == SETUP_NOT_STARTED) {
            return Error(PRECONDITION_FAILED, "Setup was not durably started");
        }
        TSecretRecord secret;
        if (const auto status = ReadSecret(db, record.GetSecret(), secret); status != EReadStatus::Ready) {
            return ReadError(status, "Secret");
        }
        if (!record.GetSetupOperationId().empty() && !request.GetSetupOperationId().empty()
            && record.GetSetupOperationId() != request.GetSetupOperationId()) {
            return Error(CONFLICT, "A known IAM setup operation ID cannot be replaced");
        }
        const TString setupOperationId = request.GetSetupOperationId().empty()
            ? record.GetSetupOperationId() : request.GetSetupOperationId();
        if (record.GetSetupState() == request.GetOutcome() && record.GetSetupOperationId() == setupOperationId) {
            return Reply(record, &secret);
        }
        if (record.GetRevision() != request.GetExpectedRevision()
            || (record.GetSetupState() != SETUP_STARTED && record.GetSetupState() != SETUP_UNKNOWN)) {
            return Error(PRECONDITION_FAILED, "Setup outcome is terminal or its revision changed");
        }
        TDatabaseRecord database;
        if (const auto status = ReadDatabase(db, record.GetDatabaseIncarnation(), database); status != EReadStatus::Ready) {
            return ReadError(status, "Database");
        }
        record.SetSetupState(request.GetOutcome());
        record.SetSetupOperationId(setupOperationId);
        if (request.GetOutcome() == SETUP_FAILED) {
            record.SetState(CANCELLED);
            if (secret.GetPendingOperationId() == record.GetOperationId()) {
                secret.ClearPendingOperationId();
                secret.SetRevision(secret.GetRevision() + 1);
                WriteSecret(db, secret);
            }
        }
        record.SetRevision(record.GetRevision() + 1);
        WriteDelegation(db, record);
        WriteDatabase(db, database);
        return Reply(record, &secret, &database);
    }

    EExecution GetSecret(NIceDb::TNiceDb& db, const TGetSecretRequest& request) {
        if (!ValidSecret(request.GetSecret())) {
            return Error(INVALID_ARGUMENT, "A complete immutable secret identity is required");
        }
        TSecretRecord secret;
        if (const auto status = ReadSecret(db, request.GetSecret(), secret); status != EReadStatus::Ready) {
            return ReadError(status, "Secret");
        }
        *Response->Record.MutableSecret() = secret;
        return EExecution::Complete;
    }

    EExecution GetDelegation(NIceDb::TNiceDb& db, const TGetDelegationRequest& request) {
        TDelegation record;
        if (const auto status = ReadDelegation(db, request.GetOperationId(), record); status != EReadStatus::Ready) {
            return ReadError(status, "Delegation");
        }
        TSecretRecord secret;
        if (record.HasSecret()) {
            if (const auto status = ReadSecret(db, record.GetSecret(), secret); status != EReadStatus::Ready) {
                return ReadError(status, "Secret");
            }
        }
        return Reply(record, record.HasSecret() ? &secret : nullptr);
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        // Execute may restart after a page fault. Do not keep a partial response.
        Response = MakeHolder<TEvIamDelegationTablet::TEvResponse>();
        Response->Record.SetStatus(SUCCESS);
        NIceDb::TNiceDb db(txc.DB);
        const auto& request = Event->Get()->Record;
        EExecution result = EExecution::Complete;
        switch (request.Command_case()) {
            case TRequest::kRegisterDatabase: result = RegisterDatabase(db, request.GetRegisterDatabase()); break;
            case TRequest::kStage: result = Stage(db, request.GetStage()); break;
            case TRequest::kBindSecret: result = BindSecret(db, request.GetBindSecret()); break;
            case TRequest::kStartSetup: result = StartSetup(db, request.GetStartSetup()); break;
            case TRequest::kSetSetupResult: result = SetSetupResult(db, request.GetSetSetupResult()); break;
            case TRequest::kGetSecret: result = GetSecret(db, request.GetGetSecret()); break;
            case TRequest::kGetDelegation: result = GetDelegation(db, request.GetGetDelegation()); break;
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
