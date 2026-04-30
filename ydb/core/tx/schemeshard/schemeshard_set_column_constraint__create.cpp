#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

class TSchemeShard::TIndexBuilder::TTxCreateSetColumnConstraint: public TSchemeShard::TIndexBuilder::TTxSimple<TEvSetColumnConstraint::TEvCreateRequest, TEvSetColumnConstraint::TEvCreateResponse> {
public:
    explicit TTxCreateSetColumnConstraint(TSelf* self, TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev)
        : TTxSimple(self, TIndexBuildId(ev->Get()->Record.GetTxId()), ev, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;

        Response = MakeHolder<TEvSetColumnConstraint::TEvCreateResponse>(request.GetTxId());

        if (!Self->EnableSetColumnConstraint) {
            return Reply(Ydb::StatusIds::UNSUPPORTED, "SetColumnConstraint feature is disabled");
        }

        if (!request.HasSettings()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Failed item check: Missing settings");
        }

        const auto& settings = request.GetSettings();
        LOG_N("DoExecute " << request.ShortDebugString());

        if (Self->SetColumnConstraintOperations.contains(BuildId)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "SetColumnConstraint operation with id '" << BuildId << "' already exists");
        }

        if (Self->IndexBuilds.contains(BuildId)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Another long-running operation with id '" << BuildId << "' already exists");
        }

        const TString& uid = GetUid(request.GetOperationParams());
        if (uid && Self->SetColumnConstraintOperationsByUid.contains(uid)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "SetColumnConstraint operation with uid '" << uid << "' already exists");
        }

        const auto domainPath = TPath::Resolve(request.GetDatabaseName(), Self);
        {
            const auto checks = domainPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsSubDomain()
                .NotUnderDomainUpgrade();

            if (!checks) {
                return Reply(checks.GetStatus(), checks.GetError());
            }
        }

        auto subDomainPathId = domainPath.GetPathIdForDomain();
        auto subDomainInfo = domainPath.DomainInfo();
        const bool quotaAcquired = subDomainInfo->TryConsumeSchemeQuota(ctx.Now());

        if (!quotaAcquired) {
            return Reply(Ydb::StatusIds::OVERLOADED,
                "Request exceeded a limit on the number of schema operations, try again later.");
        }

        NIceDb::TNiceDb db(txc.DB);
        // We need to persist updated/consumed quotas even if operation fails for other reasons
        Self->PersistSubDomainSchemeQuotas(db, subDomainPathId, *subDomainInfo);

        const auto tablePath = TPath::Resolve(settings.GetTablePath(), Self);
        {
            const auto checks = tablePath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTable()
                .NotAsyncReplicaTable()
                .IsCommonSensePath()
                .IsTheSameDomain(domainPath);

            if (!checks) {
                return Reply(checks.GetStatus(), checks.GetError());
            }
        }

        if (settings.NotNullColumnsSize() == 0) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Failed item check: There are no columns that need to be updated");
        }

        std::vector<std::string> sortedNotNullColumns(settings.GetNotNullColumns().begin(), settings.GetNotNullColumns().end());
        std::sort(sortedNotNullColumns.begin(), sortedNotNullColumns.end());

        if (const auto duplicateIt = std::adjacent_find(sortedNotNullColumns.begin(), sortedNotNullColumns.end()); duplicateIt != sortedNotNullColumns.end()) {
            TString error = TStringBuilder()
                << "Duplicate column name `" << *duplicateIt << "` in not null columns.";

            return Reply(NKikimrScheme::StatusInvalidParameter, std::move(error));
        }

        {
            Y_ABORT_UNLESS(Self->Tables.contains(tablePath.Base()->PathId));
            const auto& tableInfo = Self->Tables.at(tablePath.Base()->PathId);
            Y_ABORT_UNLESS(tableInfo);

            for (const auto& columnName : sortedNotNullColumns) {
                auto id = tableInfo->GetColumnIdByNameSlow(TString(columnName));
                if (id == TTableInfo::InvalidColumnId) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Failed item check: Column '" << columnName << "' does not exist");
                }
            }
        }

        auto operationInfo = std::make_shared<TSetColumnConstraintOperationInfo>();
        operationInfo->Id = BuildId;
        operationInfo->Uid = uid;
        operationInfo->DomainPathId = domainPath.Base()->PathId;
        operationInfo->TablePathId = tablePath.Base()->PathId;

        operationInfo->CreateSender = Request->Sender;
        operationInfo->SenderCookie = Request->Cookie;
        operationInfo->StartTime = TAppData::TimeProvider->Now();

        operationInfo->NotNullColumns = std::move(sortedNotNullColumns);

        if (request.HasUserSID()) {
            operationInfo->UserSID = request.GetUserSID();
        }

        Self->PersistCreateSetColumnConstraint(db, *operationInfo);

        operationInfo->OperationState = TSetColumnConstraintOperationInfo::EOperationState::LockTableOnSchemaOps;
        Self->PersistSetColumnConstraintState(db, *operationInfo);

        Self->AddSetColumnConstraintOperation(operationInfo);

        Progress(BuildId);

        return true;
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxCreateSetColumnConstraint(TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCreateSetColumnConstraint(this, ev);
}

}
