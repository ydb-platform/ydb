#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"

#include <ydb/core/ydb_convert/table_settings.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

class TSchemeShard::TIndexBuilder::TTxCreate: public TSchemeShard::TIndexBuilder::TTxSimple<TEvIndexBuilder::TEvCreateRequest, TEvIndexBuilder::TEvCreateResponse> {
public:
    explicit TTxCreate(TSelf* self, TEvIndexBuilder::TEvCreateRequest::TPtr& ev)
        : TTxSimple(self, ev, TXTYPE_CREATE_INDEX_BUILD)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        const auto& settings = request.GetSettings();
        LOG_N("DoExecute " << request.ShortDebugString());

        Response = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(request.GetTxId());

        const auto id = TIndexBuildId(request.GetTxId());
        if (Self->IndexBuilds.contains(id)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Index build with id '" << id << "' already exists");
        }

        const TString& uid = GetUid(request.GetOperationParams().labels());
        if (uid && Self->IndexBuildsByUid.contains(uid)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
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
                return Reply(checks.GetStatus(), checks.GetError());
            }
        }

        auto subDomainPathId = domainPath.GetPathIdForDomain();
        auto subDomainInfo = domainPath.DomainInfo();
        const bool quotaAcquired = subDomainInfo->TryConsumeSchemeQuota(ctx.Now());

        NIceDb::TNiceDb db(txc.DB);
        // We need to persist updated/consumed quotas even if operation fails for other reasons
        Self->PersistSubDomainSchemeQuotas(db, subDomainPathId, *subDomainInfo);

        if (!quotaAcquired) {
            return Reply(Ydb::StatusIds::OVERLOADED,
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
                return Reply(checks.GetStatus(), checks.GetError());
            }
        }

        TIndexBuildInfo::TPtr buildInfo = new TIndexBuildInfo(id, uid);
        buildInfo->DomainPathId = domainPath.Base()->PathId;
        buildInfo->TablePathId = tablePath.Base()->PathId;

        auto makeReply = [&] (std::string_view explain) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Failed item check: " << explain);
        };
        if (settings.has_index() && settings.has_column_build_operation()) {
            return makeReply("unable to build index and column in the single operation");
        } else if (settings.has_index()) {
            const auto& indexPath = tablePath.Child(settings.index().name());
            {
                const auto checks = indexPath.Check();
                checks
                    .IsAtLocalSchemeShard();

                if (indexPath.IsResolved()) {
                    checks
                        .IsResolved()
                        .NotUnderDeleting()
                        .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, settings.if_not_exist());
                } else {
                    checks
                        .NotEmpty()
                        .NotResolved();
                }

                if (settings.pg_mode()) {
                    checks.IsNameUniqGrandParentLevel();
                }

                checks
                    .IsValidLeafName()
                    .PathsLimit(2) // index and impl-table
                    .DirChildrenLimit()
                    .ShardsLimit(1); // impl-table

                if (!checks) {
                    return Reply(checks.GetStatus(), checks.GetError());
                }
            }

            auto tableInfo = Self->Tables.at(tablePath.Base()->PathId);
            auto domainInfo = tablePath.DomainInfo();

            const ui64 aliveIndices = Self->GetAliveChildren(
                tablePath.Base(), NKikimrSchemeOp::EPathTypeTableIndex);

            if (aliveIndices + 1 >
                domainInfo->GetSchemeLimits().MaxTableIndices) {
                return Reply(
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    TStringBuilder()
                        << "indexes count has reached maximum value in the table, "
                           "children limit for dir in domain: "
                        << domainInfo->GetSchemeLimits().MaxTableIndices
                        << ", intention to create new children: "
                        << aliveIndices + 1);
            }

            TString explain;
            if (!Prepare(*buildInfo, settings, explain)) {
                return makeReply(explain);
            }

            NKikimrSchemeOp::TIndexBuildConfig tmpConfig;
            buildInfo->SerializeToProto(Self, &tmpConfig);
            const auto indexDesc = tmpConfig.GetIndex();
            if (!NTableIndex::CommonCheck(tableInfo, indexDesc,
                                          domainInfo->GetSchemeLimits(),
                                          explain)) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, explain);
            }
        } else if (settings.has_column_build_operation()) {
            buildInfo->TargetName = settings.source_path();
            // put some validation here for the build operation
            buildInfo->BuildKind = TIndexBuildInfo::EBuildKind::BuildColumns;
            buildInfo->BuildColumns.reserve(settings.column_build_operation().column_size());
            for(int i = 0; i < settings.column_build_operation().column_size(); i++) {
                const auto& colInfo = settings.column_build_operation().column(i);
                bool notNull = colInfo.HasNotNull() && colInfo.GetNotNull();
                TString familyName = colInfo.HasFamily() ? colInfo.GetFamily() : "";
                buildInfo->BuildColumns.push_back(
                    TIndexBuildInfo::TColumnBuildInfo(
                        colInfo.GetColumnName(), colInfo.default_from_literal(), notNull, familyName));
            }
        } else {
            return makeReply("missing index or column to build");
        }

        buildInfo->Limits.MaxBatchRows = settings.max_batch_rows();
        buildInfo->Limits.MaxBatchBytes = settings.max_batch_bytes();
        buildInfo->Limits.MaxShards = settings.max_shards_in_flight();
        buildInfo->Limits.MaxRetries = settings.max_retries_upload_batch();

        buildInfo->CreateSender = Request->Sender;
        buildInfo->SenderCookie = Request->Cookie;

        Self->PersistCreateBuildIndex(db, *buildInfo);

        if (buildInfo->IsBuildColumns()) {
            buildInfo->State = TIndexBuildInfo::EState::AlterMainTable;
        } else {
            Y_ASSERT(buildInfo->IsBuildIndex());
            buildInfo->State = TIndexBuildInfo::EState::Locking;
        }

        Self->PersistBuildIndexState(db, *buildInfo);

        auto [it, emplaced] = Self->IndexBuilds.emplace(id, buildInfo);
        Y_ASSERT(emplaced);
        if (uid) {
            std::tie(std::ignore, emplaced) = Self->IndexBuildsByUid.emplace(uid, buildInfo);
            Y_ASSERT(emplaced);
        }

        Progress(id);

        return true;
    }

    void DoComplete(const TActorContext&) override {}

private:
    bool Prepare(TIndexBuildInfo& buildInfo, const NKikimrIndexBuilder::TIndexBuildSettings& settings, TString& explain) {
        Y_ASSERT(settings.has_index());
        buildInfo.BuildKind = TIndexBuildInfo::EBuildKind::BuildIndex;
        const auto& index = settings.index();

        switch (index.type_case()) {
        case Ydb::Table::TableIndex::TypeCase::kGlobalIndex:
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobal;
            break;
        case Ydb::Table::TableIndex::TypeCase::kGlobalAsyncIndex:
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync;
            break;
        case Ydb::Table::TableIndex::TypeCase::kGlobalUniqueIndex:
            explain = "unsupported index type to build";
            return false;
        case Ydb::Table::TableIndex::TypeCase::kGlobalVectorKmeansTreeIndex: {
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;
            NKikimrSchemeOp::TVectorIndexKmeansTreeDescription vectorIndexKmeansTreeDescription;
            *vectorIndexKmeansTreeDescription.MutableSettings() = index.global_vector_kmeans_tree_index().vector_settings();
            buildInfo.SpecializedIndexDescription = vectorIndexKmeansTreeDescription;
            break;
        }
        case Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET:
            explain = "invalid or unset index type";
            return false;
        };

        buildInfo.IndexName = index.name();
        buildInfo.IndexColumns.assign(index.index_columns().begin(), index.index_columns().end());
        buildInfo.DataColumns.assign(index.data_columns().begin(), index.data_columns().end());

        Ydb::StatusIds::StatusCode status;
        if (!FillIndexTablePartitioning(buildInfo.ImplTableDescriptions, index, status, explain)) {
            return false;
        } 
        return true;
    }

    static TString GetUid(const google::protobuf::Map<TString, TString>& labels) {
        auto it = labels.find("uid");
        if (it == labels.end()) {
            return TString();
        }

        return it->second;
    }
};

ITransaction* TSchemeShard::CreateTxCreate(TEvIndexBuilder::TEvCreateRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCreate(this, ev);
}

}
