#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

class TSchemeShard::TIndexBuilder::TTxCreate: public TSchemeShard::TIndexBuilder::TTxBase {
    TEvIndexBuilder::TEvCreateRequest::TPtr Request;

public:
    explicit TTxCreate(TSelf* self, TEvIndexBuilder::TEvCreateRequest::TPtr& ev)
        : TSchemeShard::TIndexBuilder::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_INDEX_BUILD;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        const auto& settings = request.GetSettings();

        LOG_N("TIndexBuilder::TTxCreate: DoExecute"
              << ", Database: " << request.GetDatabaseName()
              << ", BuildIndexId: " << request.GetTxId()
              << ", Table: " << request.GetSettings().source_path()
              << ", IndexName: " <<  request.GetSettings().index().name());
        LOG_D("Message: " << request.ShortDebugString());

        auto response = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(request.GetTxId());

        const auto id = TIndexBuildId(request.GetTxId());
        if (Self->IndexBuilds.contains(id)) {
            return Reply(std::move(response), Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Index build with id '" << id << "' already exists");
        }

        const TString& uid = GetUid(request.GetOperationParams().labels());
        if (uid && Self->IndexBuildsByUid.contains(uid)) {
            return Reply(std::move(response), Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Index build with uid '" << uid << "' already exists");
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
                return Reply(std::move(response), TranslateStatusCode(checks.GetStatus()), checks.GetError());
            }
        }

        auto subDomainPathId = domainPath.GetPathIdForDomain();
        auto subDomainInfo = domainPath.DomainInfo();
        const bool quotaAcquired = subDomainInfo->TryConsumeSchemeQuota(ctx.Now());

        NIceDb::TNiceDb db(txc.DB);
        // We need to persist updated/consumed quotas even if operation fails for other reasons
        Self->PersistSubDomainSchemeQuotas(db, subDomainPathId, *subDomainInfo);

        if (!quotaAcquired) {
            return Reply(std::move(response), Ydb::StatusIds::OVERLOADED,
                "Request exceeded a limit on the number of schema operations, try again later.");
        }

        const auto tablePath = TPath::Resolve(settings.source_path(), Self);
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
                return Reply(std::move(response), TranslateStatusCode(checks.GetStatus()), checks.GetError());
            }
        }

        const auto indexPath = tablePath.Child(settings.index().name());
        {
            const auto checks = indexPath.Check();
            checks
                .IsAtLocalSchemeShard();

            if (indexPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, false);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            checks
                .IsValidLeafName()
                .PathsLimit(2) // index and impl-table
                .DirChildrenLimit()
                .ShardsLimit(1); // impl-table

            if (!checks) {
                return Reply(std::move(response), TranslateStatusCode(checks.GetStatus()), checks.GetError());
            }
        }

        auto tableInfo = Self->Tables.at(tablePath.Base()->PathId);
        auto domainInfo = tablePath.DomainInfo();

        const ui64 aliveIndices = Self->GetAliveChildren(tablePath.Base(), NKikimrSchemeOp::EPathTypeTableIndex);
        if (aliveIndices + 1 > domainInfo->GetSchemeLimits().MaxTableIndices) {
            return Reply(std::move(response), Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder()
                << "indexes count has reached maximum value in the table"
                << ", children limit for dir in domain: " << domainInfo->GetSchemeLimits().MaxTableIndices
                << ", intention to create new children: " << aliveIndices + 1);
        }

        TIndexBuildInfo::TPtr buildInfo = new TIndexBuildInfo(id, uid);

        TString explain;
        if (!Prepare(buildInfo, domainPath, tablePath, settings, explain)) {
            return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                << "Failed item check: " << explain);
        }

        const auto indexDesc = buildInfo->SerializeToProto(Self).GetIndex();
        if (!NTableIndex::CommonCheck(tableInfo, indexDesc, domainInfo->GetSchemeLimits(), explain)) {
            return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, explain);
        }

        Y_VERIFY(buildInfo != nullptr);

        buildInfo->CreateSender = Request->Sender;
        buildInfo->SenderCookie = Request->Cookie;

        Self->PersistCreateBuildIndex(db, buildInfo);

        buildInfo->State = TIndexBuildInfo::EState::Locking;
        Self->PersistBuildIndexState(db, buildInfo);

        Self->IndexBuilds[id] = buildInfo;
        if (uid) {
            Self->IndexBuildsByUid[uid] = buildInfo;
        }

        Progress(id);

        return true;
    }

    void DoComplete(const TActorContext&) override {
        LOG_D("TIndexBuilder::TTxCreate: DoComplete");
    }

private:
    bool Prepare(TIndexBuildInfo::TPtr buildInfo, const TPath& database, const TPath& path, const NKikimrIndexBuilder::TIndexBuildSettings& settings, TString& explain) {
        buildInfo->DomainPathId = database.Base()->PathId;
        buildInfo->TablePathId = path.Base()->PathId;

        switch (settings.index().type_case()) {
        case Ydb::Table::TableIndex::TypeCase::kGlobalIndex:
            buildInfo->IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobal;
            break;
        case Ydb::Table::TableIndex::TypeCase::kGlobalAsyncIndex:
            buildInfo->IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync;
            break;
        case Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET:
            explain = "invalid or unset index type";
            return false;
        };

        buildInfo->IndexName = settings.index().name();
        buildInfo->IndexColumns.assign(settings.index().index_columns().begin(), settings.index().index_columns().end());
        buildInfo->DataColumns.assign(settings.index().data_columns().begin(), settings.index().data_columns().end());

        buildInfo->Limits.MaxBatchRows = settings.max_batch_rows();
        buildInfo->Limits.MaxBatchBytes = settings.max_batch_bytes();
        buildInfo->Limits.MaxShards = settings.max_shards_in_flight();
        buildInfo->Limits.MaxRetries = settings.max_retries_upload_batch();

        return true;
    }

    static TString GetUid(const google::protobuf::Map<TString, TString>& labels) {
        auto it = labels.find("uid");
        if (it == labels.end()) {
            return TString();
        }

        return it->second;
    }

    bool Reply(THolder<TEvIndexBuilder::TEvCreateResponse> responseEv,
               const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
               const TString& errorMessage = TString())
    {
        LOG_N("TIndexBuilder::TTxCreate: Reply"
              << ", BuildIndexId: " << responseEv->Record.GetTxId()
              << ", status: " << status
              << ", error: " << errorMessage);
        LOG_D("Message: " << responseEv->Record.ShortDebugString());

        auto& record = responseEv->Record;
        record.SetStatus(status);
        if (errorMessage) {
            AddIssue(record.MutableIssues(), errorMessage);
        }

        Send(Request->Sender, std::move(responseEv), 0, Request->Cookie);

        return true;
    }
};

ITransaction* TSchemeShard::CreateTxCreate(TEvIndexBuilder::TEvCreateRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCreate(this, ev);
}

}
