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

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        // Execute may restart after a page fault. Do not keep a partial response.
        Response = MakeHolder<TEvIamDelegationTablet::TEvResponse>();
        Response->Record.SetStatus(SUCCESS);
        NIceDb::TNiceDb db(txc.DB);
        const auto& request = Event->Get()->Record;
        EExecution result = EExecution::Complete;
        switch (request.Command_case()) {
            case TRequest::kRegisterDatabase: result = RegisterDatabase(db, request.GetRegisterDatabase()); break;
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
