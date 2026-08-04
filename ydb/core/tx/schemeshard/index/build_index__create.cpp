#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_xxport__helpers.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/ydb_convert/table_settings.h>

namespace NKikimr::NSchemeShard {

static constexpr ui32 DefaultMaxShardsInFlight = 32;

using namespace NTabletFlatExecutor;
using NKikimrSchemeOp::EIndexType;

class TSchemeShard::TIndexBuilder::TTxCreate: public TSchemeShard::TIndexBuilder::TTxSimple<TEvIndexBuilder::TEvCreateRequest, TEvIndexBuilder::TEvCreateResponse> {
public:
    explicit TTxCreate(TSelf* self, TEvIndexBuilder::TEvCreateRequest::TPtr& ev)
        : TTxSimple(self, TIndexBuildId(ev->Get()->Record.GetTxId()), ev, TXTYPE_CREATE_INDEX_BUILD)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        const auto& settings = request.GetSettings();
        LOG_N("DoExecute " << request.ShortDebugString());

        Response = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(request.GetTxId());

        if (Self->IndexBuilds.contains(BuildId)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Index build with id '" << BuildId << "' already exists");
        }

        if (Self->SetColumnConstraintOperations.contains(BuildId)) {
            return Reply(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Another long-running operation with id '" << BuildId << "' already exists");
        }

        const TString& uid = GetUid(request.GetOperationParams());
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

        auto buildInfo = std::make_shared<TIndexBuildInfo>();
        buildInfo->Id = BuildId;
        buildInfo->Uid = uid;
        buildInfo->DomainPathId = domainPath.Base()->PathId;
        buildInfo->TablePathId = tablePath.Base()->PathId;

        auto makeReply = [&] (std::string_view explain) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Failed item check: " << explain);
        };
        if (settings.has_index() && settings.has_column_build_operation()) {
            return makeReply("unable to build index and column in the single operation");
        } else if (settings.has_index()) {
            const bool isRebuild = settings.is_rebuild();
            const auto& indexPath = tablePath.Child(settings.index().name());
            if (isRebuild) {
                // For REBUILD INDEX, the index must already exist and be Ready
                const auto checks = indexPath.Check();
                checks
                    .IsAtLocalSchemeShard()
                    .IsResolved()
                    .NotDeleted()
                    .NotUnderDeleting();

                if (!checks) {
                    return Reply(checks.GetStatus(), TStringBuilder()
                        << "REBUILD INDEX: index '" << settings.index().name() << "' check failed: " << checks.GetError());
                }

                if (indexPath.Base()->PathType != TPathElement::EPathType::EPathTypeTableIndex) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                        << "REBUILD INDEX: '" << settings.index().name() << "' is not an index");
                }

                buildInfo->IsRebuild = true;
            } else {
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
                    .IsValidLeafName(/*userToken*/ nullptr)
                    .DirChildrenLimit();

                if (!checks) {
                    return Reply(checks.GetStatus(), checks.GetError());
                }
            }

            auto tableInfo = Self->Tables.at(tablePath.Base()->PathId);
            auto domainInfo = tablePath.DomainInfo();

            if (!isRebuild) {
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
            }

            TString explain;
            if (!Prepare(*buildInfo, settings, tableInfo, explain)) {
                return makeReply(explain);
            }

            if (tableInfo->IsTTLEnabled() && !DoesIndexSupportTTL(buildInfo->IndexType)) {
                return Reply(Ydb::StatusIds::PRECONDITION_FAILED,
                    TStringBuilder() << "Table with " << buildInfo->IndexType << " index doesn't support TTL");
            }

            NKikimrSchemeOp::TIndexBuildConfig tmpConfig;
            buildInfo->SerializeToProto(Self, &tmpConfig);
            auto& indexDesc = *tmpConfig.MutableIndex();

            // Decide how a fulltext index on this table obtains its doc_id. For a custom (non single
            // integer) PK without the rowid infrastructure we auto-provision it: the build first spawns
            // child builds for the __ydb_row_id column and/or the unique index on it (under this build's
            // shared lock), then builds the fulltext index in rowid mode. See the provisioning prefix
            // in build_index__progress.cpp.
            const auto classification = NTableIndex::ClassifyFulltextRowId(
                tableInfo, tablePath.Base()->GetChildren(), Self->Indexes, indexDesc, explain);
            auto enableRowIdMode = [&]() {
                indexDesc.MutableFulltextIndexDescription()->SetUseRowIdAsDocId(true);
                // Fulltext index builds always carry a TFulltextIndexDescription. JSON index builds
                // historically carry std::monostate (no settings); attach a fulltext description here
                // so the UseRowIdAsDocId flag is persisted and propagated like for fulltext.
                if (auto* ft = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&buildInfo->SpecializedIndexDescription)) {
                    ft->SetUseRowIdAsDocId(true);
                } else {
                    NKikimrSchemeOp::TFulltextIndexDescription ftd;
                    ftd.SetUseRowIdAsDocId(true);
                    buildInfo->SpecializedIndexDescription = std::move(ftd);
                }
            };
            switch (classification.Plan) {
                case NTableIndex::EFulltextRowIdPlan::Error:
                    return Reply(Ydb::StatusIds::BAD_REQUEST, explain);
                case NTableIndex::EFulltextRowIdPlan::NotApplicable:
                case NTableIndex::EFulltextRowIdPlan::LegacyIntegerPk:
                    break;
                case NTableIndex::EFulltextRowIdPlan::Reuse:
                    enableRowIdMode();
                    break;
                case NTableIndex::EFulltextRowIdPlan::Provision: {
                    if (!Self->EnableAddUniqueIndex) {
                        return Reply(Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder()
                            << "Auto-provisioning '" << NTableIndex::NFulltext::RowIdColumn
                            << "' for a fulltext/JSON index on a non-integer-PK table requires the unique-index feature");
                    }
                    buildInfo->FulltextNeedsRowIdColumn = classification.NeedColumn;
                    buildInfo->FulltextNeedsUniqueIndex = classification.NeedUniqueIndex;
                    buildInfo->AutoUniqueIndexName = NTableIndex::NFulltext::RowIdUniqueIndexName;
                    if (classification.NeedUniqueIndex) {
                        // The auto unique-index path must be free (a reusable Ready index would have been
                        // classified as Reuse, not Provision).
                        const auto autoUniquePath = tablePath.Child(buildInfo->AutoUniqueIndexName);
                        if (autoUniquePath.IsResolved() && !autoUniquePath.IsDeleted()) {
                            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                                << "Cannot auto-provision the fulltext rowid unique index: path '"
                                << autoUniquePath.PathString() << "' already exists");
                        }
                    }
                    // The fulltext index runs in rowid mode once provisioning completes.
                    enableRowIdMode();
                    break;
                }
            }
            if (!NTableIndex::CommonCheck(tableInfo, indexDesc,
                                          domainInfo->GetSchemeLimits(),
                                          explain)) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, explain);
            }

            {
                const auto checks = indexPath.Check();

                // Tables are actually created in schemeshard__operation_create_build_index so limits are rechecked there too
                auto counts = NTableIndex::GetIndexObjectCounts(indexDesc);
                if (counts.SequenceCount > 0 && domainInfo->GetSequenceShards().empty()) {
                    ++counts.IndexTableShards;
                }

                checks.PathsLimit(1 + counts.IndexTableCount + counts.SequenceCount);
                if (!request.GetInternal()) {
                    checks
                        .ShardsLimit(counts.IndexTableShards)
                        .PathShardsLimit(counts.ShardsPerPath);
                }

                if (!checks) {
                    return Reply(checks.GetStatus(), checks.GetError());
                }
            }
        } else if (settings.has_column_build_operation()) {
            bool allFromSequence = settings.column_build_operation().column_size() > 0;
            for (int i = 0; i < settings.column_build_operation().column_size(); i++) {
                if (settings.column_build_operation().column(i).default_from_sequence().empty()) {
                    allFromSequence = false;
                    break;
                }
            }
            if (!Self->EnableAddColumsWithDefaults && !allFromSequence) {
                return Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Adding columns with defaults is disabled");
            }

            buildInfo->TargetName = tablePath.PathString();
            // put some validation here for the build operation
            buildInfo->BuildKind = TIndexBuildInfo::EBuildKind::BuildColumns;
            buildInfo->BuildColumns.reserve(settings.column_build_operation().column_size());
            for(int i = 0; i < settings.column_build_operation().column_size(); i++) {
                const auto& colInfo = settings.column_build_operation().column(i);
                bool notNull = colInfo.HasNotNull() && colInfo.GetNotNull();
                TString familyName = colInfo.HasFamily() ? colInfo.GetFamily() : "";
                if (!colInfo.default_from_sequence().empty()) {
                    buildInfo->BuildColumns.push_back(
                        TIndexBuildInfo::TColumnBuildInfo(
                            TIndexBuildInfo::TColumnBuildInfo::FromSequenceTag{},
                            colInfo.GetColumnName(),
                            colInfo.default_from_literal().type(),
                            colInfo.default_from_sequence(),
                            colInfo.bit_reverse_sequence_value(),
                            notNull,
                            familyName));
                } else {
                    buildInfo->BuildColumns.push_back(
                        TIndexBuildInfo::TColumnBuildInfo(
                            colInfo.GetColumnName(), colInfo.default_from_literal(), notNull, familyName));
                }
            }
        } else {
            return makeReply("missing index or column to build");
        }

        if (!request.GetInternal() && settings.max_shards_in_flight() > Self->MaxBuildIndexShardsInFlight) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder()
                    << "maximum allowed build parallelism is " << Self->MaxBuildIndexShardsInFlight
                    << ", but requested " << settings.max_shards_in_flight());
        }

        if (Self->MaxStoredIndexBuilds > 0 &&
            Self->IndexBuilds.size() >= Self->MaxStoredIndexBuilds) {
            // Remove oldest items from IndexBuilds
            std::vector<std::shared_ptr<TIndexBuildInfo>> toErase;
            for (auto& [timestamp, id]: Self->IndexBuildsByTime) {
                auto olderBuild = Self->IndexBuilds.at(id);
                if (olderBuild->IsFinished()) {
                    toErase.push_back(olderBuild);
                    if (Self->IndexBuilds.size() - toErase.size() < Self->MaxStoredIndexBuilds) {
                        break;
                    }
                }
            }
            for (auto& olderBuild: toErase) {
                if (!Self->PersistBuildIndexForget(db, *olderBuild)) {
                    return false;
                }
            }
            for (auto& olderBuild: toErase) {
                EraseBuildInfo(*olderBuild);
            }
        }

        buildInfo->ScanSettings.CopyFrom(settings.GetScanSettings());
        if (settings.max_shards_in_flight() > 0) {
            buildInfo->MaxInProgressShards = settings.max_shards_in_flight();
        } else if (Self->MaxBuildIndexShardsInFlight > DefaultMaxShardsInFlight) {
            buildInfo->MaxInProgressShards = DefaultMaxShardsInFlight;
        } else if (Self->MaxBuildIndexShardsInFlight > 0) {
            buildInfo->MaxInProgressShards = Self->MaxBuildIndexShardsInFlight;
        } else {
            buildInfo->MaxInProgressShards = 1;
        }

        buildInfo->CreateSender = Request->Sender;
        buildInfo->SenderCookie = Request->Cookie;
        buildInfo->StartTime = TAppData::TimeProvider->Now();
        if (request.HasUserSID()) {
            buildInfo->UserSID = request.GetUserSID();
        }

        Self->PersistCreateBuildIndex(db, *buildInfo);

        if (buildInfo->IsFulltextProvisioning()) {
            Self->PersistBuildIndexFulltextProvisioning(db, *buildInfo);
            // Provision the rowid infrastructure (sequentially, via child builds) before this build
            // takes its own lock and builds the fulltext index.
            buildInfo->State = buildInfo->FulltextNeedsRowIdColumn
                ? TIndexBuildInfo::EState::ProvisioningRowIdColumn
                : TIndexBuildInfo::EState::ProvisioningRowIdUniqueIndex;
        } else {
            buildInfo->State = TIndexBuildInfo::EState::Locking;
        }

        Self->PersistBuildIndexState(db, *buildInfo);
        Self->AddIndexBuild(buildInfo);

        Progress(BuildId);

        return true;
    }

    void DoComplete(const TActorContext&) override {}

private:
    bool Prepare(TIndexBuildInfo& buildInfo, const NKikimrIndexBuilder::TIndexBuildSettings& settings,
                 TTableInfo::TPtr tableInfo, TString& explain) {
        Y_ASSERT(settings.has_index());
        const auto& index = settings.index();

        switch (index.type_case()) {
        case Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET:
            explain = "Invalid or unset index type";
            return false;
        case Ydb::Table::TableIndex::TypeCase::kGlobalIndex:
            buildInfo.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobal;
            break;
        case Ydb::Table::TableIndex::TypeCase::kGlobalAsyncIndex:
            buildInfo.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync;
            break;
        case Ydb::Table::TableIndex::TypeCase::kGlobalUniqueIndex: {
            if (!Self->EnableAddUniqueIndex) {
                explain = "Adding a unique index to an existing table is disabled";
                return false;
            }
            buildInfo.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryUniqueIndex;
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique;
            break;
        }
        case Ydb::Table::TableIndex::TypeCase::kGlobalVectorKmeansTreeIndex: {
            buildInfo.BuildKind = index.index_columns().size() == 1
                ? TIndexBuildInfo::EBuildKind::BuildVectorIndex
                : TIndexBuildInfo::EBuildKind::BuildPrefixedVectorIndex;
            buildInfo.IndexType = NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;
            NKikimrSchemeOp::TVectorIndexKmeansTreeDescription vectorIndexKmeansTreeDescription;

            if (buildInfo.IsRebuild) {
                // For rebuild: start from existing index settings, then merge user overrides
                const auto& indexPath = TPath::Resolve(settings.source_path(), Self).Child(index.name());
                Y_ENSURE(indexPath.IsResolved());
                auto existingIndex = Self->Indexes.at(indexPath.Base()->PathId);
                const auto* existingDesc = std::get_if<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
                    &existingIndex->SpecializedIndexDescription);
                if (!existingDesc) {
                    explain = "REBUILD INDEX is only supported for vector_kmeans_tree indexes";
                    return false;
                }
                vectorIndexKmeansTreeDescription = *existingDesc;
                // Merge user-provided settings over existing ones
                const auto& userSettings = index.global_vector_kmeans_tree_index().vector_settings();
                if (userSettings.has_settings()) {
                    const auto& userVectorSettings = userSettings.settings();
                    const auto& existingVectorSettings = existingDesc->GetSettings().settings();
                    if (userVectorSettings.has_metric() && userVectorSettings.metric() != existingVectorSettings.metric()) {
                        explain = "REBUILD INDEX cannot change metric (distance/similarity)";
                        return false;
                    }
                    if (userVectorSettings.has_vector_type() && userVectorSettings.vector_type() != existingVectorSettings.vector_type()) {
                        explain = "REBUILD INDEX cannot change vector_type";
                        return false;
                    }
                    if (userVectorSettings.has_vector_dimension() && userVectorSettings.vector_dimension() != existingVectorSettings.vector_dimension()) {
                        explain = "REBUILD INDEX cannot change vector_dimension";
                        return false;
                    }
                }
                vectorIndexKmeansTreeDescription.MutableSettings()->MergeFrom(userSettings);
            } else {
                *vectorIndexKmeansTreeDescription.MutableSettings() = index.global_vector_kmeans_tree_index().vector_settings();
            }

            if (!NKikimr::NKMeans::ValidateSettingsPartial(vectorIndexKmeansTreeDescription.GetSettings(), explain)) {
                return false;
            }

            if (!NKikimr::NKMeans::ValidateSettings(vectorIndexKmeansTreeDescription.GetSettings(), explain)) {
                ui64 rowCount = tableInfo->GetStats().Aggregated.RowCount;
                const bool isPrefixed = index.index_columns().size() > 1;
                NKikimr::NKMeans::AutoSelectKMeansSettings(*vectorIndexKmeansTreeDescription.MutableSettings(), rowCount, isPrefixed);
                if (isPrefixed) {
                    vectorIndexKmeansTreeDescription.MutableSettings()->set_adaptive_clusters(true);
                }
            }

            const auto& kmSettings = vectorIndexKmeansTreeDescription.GetSettings();
            const auto& vectorSettings = kmSettings.settings();
            const bool needVectorAutodetect = NKikimr::NKMeans::NeedsVectorSettingsAutoSelect(vectorSettings);
            if (!NKikimr::NKMeans::ValidateSettings(kmSettings, explain) && !needVectorAutodetect) {
                return false;
            }

            buildInfo.SpecializedIndexDescription = vectorIndexKmeansTreeDescription;
            buildInfo.KMeans.K = kmSettings.clusters();
            buildInfo.KMeans.Levels = buildInfo.IsBuildPrefixedVectorIndex() + kmSettings.levels();
            buildInfo.KMeans.IsPrefixed = buildInfo.IsBuildPrefixedVectorIndex();
            buildInfo.KMeans.Adaptive = kmSettings.adaptive_clusters() && buildInfo.IsBuildPrefixedVectorIndex();
            buildInfo.KMeans.Rounds = NTableIndex::NKMeans::DefaultKMeansRounds;
            buildInfo.KMeans.OverlapClusters = kmSettings.overlap_clusters()
                ? kmSettings.overlap_clusters()
                : NTableIndex::NKMeans::DefaultOverlapClusters;
            buildInfo.KMeans.OverlapRatio = kmSettings.has_overlap_ratio()
                ? kmSettings.overlap_ratio()
                : NTableIndex::NKMeans::DefaultOverlapRatio;

            if (!needVectorAutodetect) {
                buildInfo.Clusters = NKikimr::NKMeans::CreateClusters(vectorSettings, buildInfo.KMeans.Rounds, explain);
            } else {
                buildInfo.KMeans.NeedVectorAutodetect = true;
                buildInfo.Clusters = nullptr;
            }
            break;
        }
        case Ydb::Table::TableIndex::TypeCase::kGlobalFulltextPlainIndex: {
            auto type = Self->EnableCompactFulltextIndex
                ? EIndexType::EIndexTypeGlobalFulltextCompact
                : EIndexType::EIndexTypeGlobalFulltextPlain;
            if (!PrepareFulltext(buildInfo, type, index.global_fulltext_plain_index().fulltext_settings(), explain)) {
                return false;
            }
            break;
        }
        case Ydb::Table::TableIndex::TypeCase::kGlobalFulltextRelevanceIndex: {
            auto type = Self->EnableCompactFulltextIndex
                ? EIndexType::EIndexTypeGlobalFulltextCompactRelevance
                : EIndexType::EIndexTypeGlobalFulltextRelevance;
            if (!PrepareFulltext(buildInfo, type, index.global_fulltext_relevance_index().fulltext_settings(), explain)) {
                return false;
            }
            break;
        }
        case Ydb::Table::TableIndex::TypeCase::kGlobalJsonIndex: {
            if (!Self->EnableJsonIndex) {
                explain = "JSON index support is disabled";
                return false;
            }
            buildInfo.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
            buildInfo.IndexType = Self->EnableCompactFulltextIndex
                ? NKikimrSchemeOp::EIndexType::EIndexTypeGlobalJsonCompact
                : NKikimrSchemeOp::EIndexType::EIndexTypeGlobalJson;
            break;
        }
        case Ydb::Table::TableIndex::TypeCase::kLocalBloomFilterIndex:
        case Ydb::Table::TableIndex::TypeCase::kLocalBloomNgramFilterIndex:
            explain = "Local bloom indexes are not supported by index build operation";
            return false;
        case Ydb::Table::TableIndex::TypeCase::kLocalMinMaxIndex:
            explain = "Local min_max index is not supported by index build operation";
            return false;
        };

        buildInfo.IndexName = index.name();
        buildInfo.IndexColumns.assign(index.index_columns().begin(), index.index_columns().end());
        buildInfo.DataColumns.assign(index.data_columns().begin(), index.data_columns().end());
        if (buildInfo.IsRebuild && (index.index_columns().empty() || index.data_columns().empty())) {
            // Inherit columns from the existing index when they are not provided explicitly.
            // Index columns and data columns are inherited independently: providing one must
            // not silently drop the other (e.g. specifying index_columns without data_columns
            // must preserve the existing data columns).
            const auto& indexPath = TPath::Resolve(settings.source_path(), Self).Child(index.name());
            auto existingIndex = Self->Indexes.at(indexPath.Base()->PathId);
            if (index.index_columns().empty()) {
                buildInfo.IndexColumns.assign(existingIndex->IndexKeys.begin(), existingIndex->IndexKeys.end());
            }
            if (index.data_columns().empty()) {
                buildInfo.DataColumns.assign(existingIndex->IndexDataColumns.begin(), existingIndex->IndexDataColumns.end());
            }
        }

        // Re-evaluate BuildKind for vector indexes after column inheritance
        if (buildInfo.IsBuildVectorIndex()) {
            buildInfo.BuildKind = buildInfo.IndexColumns.size() == 1
                ? TIndexBuildInfo::EBuildKind::BuildVectorIndex
                : TIndexBuildInfo::EBuildKind::BuildPrefixedVectorIndex;
        }

        Ydb::StatusIds::StatusCode status;
        if (!FillIndexTablePartitioning(buildInfo.ImplTableDescriptions, index, status, explain)) {
            return false;
        }
        return true;
    }

    bool PrepareFulltext(TIndexBuildInfo& buildInfo, NKikimrSchemeOp::EIndexType indexType, const Ydb::Table::FulltextIndexSettings& settings, TString& explain) {
        if (!Self->EnableFulltextIndex) {
            explain = "Fulltext index support is disabled";
            return false;
        }
        NKikimrSchemeOp::TFulltextIndexDescription fulltextIndexDescription;
        *fulltextIndexDescription.MutableSettings() = settings;
        buildInfo.IndexType = indexType;
        buildInfo.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        if (!NKikimr::NFulltext::ValidateSettings(fulltextIndexDescription.GetSettings(), explain)) {
            return false;
        }
        buildInfo.SpecializedIndexDescription = fulltextIndexDescription;
        return true;
    }
};

ITransaction* TSchemeShard::CreateTxCreate(TEvIndexBuilder::TEvCreateRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCreate(this, ev);
}

}
