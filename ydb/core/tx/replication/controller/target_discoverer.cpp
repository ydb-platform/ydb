#include "logging.h"
#include "private_events.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "target_topic.h"
#include "target_transfer.h"
#include "util.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/metrics_config.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/naming_conventions/scheme_object_names.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

    std::optional<NKikimrSchemeOp::EIndexType> ConvertIndexType(NYdb::NTable::EIndexType type, bool enableCompactFulltext) {
        // The SDK does not distinguish compact fulltext indexes from regular ones, while the set
        // of their impl tables differs (e.g. a compact relevance index has no dict table).
        switch (type) {
        case NYdb::NTable::EIndexType::GlobalSync:
            return NKikimrSchemeOp::EIndexTypeGlobal;
        case NYdb::NTable::EIndexType::GlobalAsync:
            return NKikimrSchemeOp::EIndexTypeGlobalAsync;
        case NYdb::NTable::EIndexType::GlobalUnique:
            return NKikimrSchemeOp::EIndexTypeGlobalUnique;
        case NYdb::NTable::EIndexType::GlobalVectorKMeansTree:
            return NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree;
        case NYdb::NTable::EIndexType::GlobalHnsw:
            return NKikimrSchemeOp::EIndexTypeGlobalHnsw;
        case NYdb::NTable::EIndexType::GlobalFulltextPlain:
            return enableCompactFulltext
                ? NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact
                : NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        case NYdb::NTable::EIndexType::GlobalFulltextRelevance:
            return enableCompactFulltext
                ? NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance
                : NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        case NYdb::NTable::EIndexType::GlobalJson:
            return enableCompactFulltext
                ? NKikimrSchemeOp::EIndexTypeGlobalJsonCompact
                : NKikimrSchemeOp::EIndexTypeGlobalJson;
        default:
            // Local indexes are a part of the table itself, there is nothing to replicate.
            return std::nullopt;
        }
    }

} // anonymous

class TTargetDiscoverer: public TActorBootstrapped<TTargetDiscoverer> {
    void DescribePath(ui32 idx) {
        Y_ABORT_UNLESS(idx < Paths.size());
        Send(YdbProxy, new TEvYdbProxy::TEvDescribePathRequest(Paths.at(idx).first, {}), 0, idx);
        Pending.insert(idx);
    }

    void DescribeTopicParent(ui32 idx) {
        const auto& path = Paths.at(idx).first;
        const bool isAbsolutePath = IsStartWithSlash(path);
        const auto absolutePath = CanonizePath(isAbsolutePath ? path
            : Config.GetSrcConnectionParams().GetDatabase() + "/" + path);
        Send(YdbProxy, new TEvYdbProxy::TEvDescribePathRequest(TString(ExtractParent(absolutePath)), {}), 0, idx);
        TopicParents.insert(idx);
    }

    void Handle(TEvYdbProxy::TEvDescribePathResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            YDB_LOG_WARN("Unknown describe path response",
                {"cookie", ev->Cookie});
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const bool isTopicParent = TopicParents.erase(*it);
        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            YDB_LOG_DEBUG("Describe path succeeded",
                {"path", path.first});

            const auto& entry = result.GetEntry();
            if (isTopicParent) {
                // DescribePath exposes a CDC stream as its hidden topic. Its parent is
                // a table; the hidden topic's parent is the stream, also exposed as Topic.
                if (!EqualToOneOf(entry.Type, NYdb::NScheme::ESchemeEntryType::Table,
                    NYdb::NScheme::ESchemeEntryType::Topic))
                {
                    return DescribeTopic(ev->Cookie);
                }
                return RejectTarget(ev->Cookie, "CDC topics cannot be replication sources");
            }
            switch (entry.Type) {
            case NYdb::NScheme::ESchemeEntryType::SubDomain:
            case NYdb::NScheme::ESchemeEntryType::Directory:
                if (IsReplication()) {
                    Pending.erase(it);
                    return ListDirectory(path);
                }
                break;
            case NYdb::NScheme::ESchemeEntryType::Table:
                if (IsReplication()) {
                    return DescribeTable(ev->Cookie);
                }
                break;
            case NYdb::NScheme::ESchemeEntryType::Topic:
                if (IsReplication()) {
                    if (!AppData()->FeatureFlags.GetEnableTopicReplication()) {
                        return RejectTarget(ev->Cookie, "Topic replication is disabled by EnableTopicReplication");
                    }
                    return DescribeTopicParent(ev->Cookie);
                }
                if (IsTransfer()) {
                    return DescribeTopic(ev->Cookie);
                }
                break;
            default:
                break;
            }

            YDB_LOG_WARN("Unsupported entry type",
                {"path", path.first},
                {"type", entry.Type});

            return RejectTarget(ev->Cookie, TStringBuilder() << "Unsupported entry type: " << entry.Type);
        } else {
            YDB_LOG_ERROR("Describe path failed",
                {"path", path.first},
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()},
                {"iteration", Backoff.GetIteration()});

            if (IsRetryableError(result, Backoff)) {
                return RetryDescribe(*it);
            } else {
                Failed.emplace_back(path.first, result);
            }
        }

        Pending.erase(it);
        MaybeReply();
    }

    void DescribeTable(ui32 idx) {
        Y_ABORT_UNLESS(idx < Paths.size());
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(Paths.at(idx).first, {}), 0, idx);
        Pending.insert(idx);
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            YDB_LOG_WARN("Unknown describe table response",
                {"cookie", ev->Cookie});
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            YDB_LOG_DEBUG("Describe table succeeded",
                {"path", path.first});

            const auto& target = ToAdd.emplace_back(TReplication::ETargetKind::Table,
                std::make_shared<TTargetTable::TTableConfig>(path.first, path.second));
            YDB_LOG_INFO("Add target",
                {"srcPath", target.Config->GetSrcPath()},
                {"dstPath", target.Config->GetDstPath()},
                {"kind", target.Kind});

            for (const auto& index : result.GetTableDescription().GetIndexDescriptions()) {
                const auto indexSrcPath = CanonizePath(ChildPath(SplitPath(path.first),
                    TString{index.GetIndexName()}));
                const auto indexDstPath = CanonizePath(ChildPath(SplitPath(path.second),
                    TString{index.GetIndexName()}));

                if (index.GetIndexType() != NYdb::NTable::EIndexType::GlobalAsync) {
                    // Async indexes are maintained by the destination's own change exchange.
                    // Other index types (fulltext, vector, json) are replicated as their impl
                    // tables: their names are defined by the index type and its keys.
                    const auto indexType = ConvertIndexType(index.GetIndexType(),
                        AppData()->FeatureFlags.GetEnableCompactFulltextIndex());
                    if (!indexType) {
                        YDB_LOG_DEBUG("Skip target for local index",
                            {"path", indexSrcPath},
                            {"type", index.GetIndexType()});
                        continue;
                    }

                    TVector<TString> indexColumns;
                    for (const auto& column : index.GetIndexColumns()) {
                        indexColumns.emplace_back(column.data(), column.size());
                    }

                    if (*indexType == NKikimrSchemeOp::EIndexTypeGlobal
                        || *indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique)
                    {
                        // Secondary indexes are replicated as a whole: the source is the index and
                        // the stream lives on its only impl table.
                        const auto& target = ToAdd.emplace_back(
                            TReplication::ETargetKind::IndexTable,
                            std::make_shared<TTargetIndexTable::TIndexTableConfig>(
                                indexSrcPath,
                                CanonizePath(TStringBuilder() << indexDstPath << '/' << NTableIndex::ImplTable)));
                        YDB_LOG_INFO("Add target",
                            {"srcPath", target.Config->GetSrcPath()},
                            {"dstPath", target.Config->GetDstPath()},
                            {"kind", target.Kind});
                        continue;
                    }

                    for (const auto implTable : NTableIndex::GetImplTables(*indexType, indexColumns)) {
                        const auto& target = ToAdd.emplace_back(
                            TReplication::ETargetKind::IndexTable,
                            std::make_shared<TTargetIndexTable::TIndexTableConfig>(
                                CanonizePath(TStringBuilder() << indexSrcPath << '/' << implTable),
                                CanonizePath(TStringBuilder() << indexDstPath << '/' << implTable)));
                        YDB_LOG_INFO("Add target",
                            {"srcPath", target.Config->GetSrcPath()},
                            {"dstPath", target.Config->GetDstPath()},
                            {"kind", target.Kind});
                    }
                }
            }
        } else {
            YDB_LOG_ERROR("Describe table failed",
                {"path", path.first},
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});

            if (IsRetryableError(result, Backoff)) {
                return RetryDescribe(*it);
            } else {
                Failed.emplace_back(path.first, result);
            }
        }

        Pending.erase(it);
        MaybeReply();
    }

    void DescribeTopic(ui32 idx) {
        Y_ABORT_UNLESS(idx < Paths.size());
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(Paths.at(idx).first, {}), 0, idx);
        Pending.insert(idx);
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            YDB_LOG_WARN("Unknown describe topic response",
                {"cookie", ev->Cookie});
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            YDB_LOG_DEBUG("Describe topic succeeded",
                {"path", path.first});

            if (IsReplication()) {
                TString error;
                if (!CheckTopic(result.GetTopicDescription(), error)) {
                    return RejectTarget(ev->Cookie, error);
                }
                ToAdd.emplace_back(TReplication::ETargetKind::Topic,
                    std::make_shared<TTargetTopic::TTopicConfig>(path.first, path.second));
            } else {
                ToAdd.emplace_back(TReplication::ETargetKind::Transfer,
                    std::make_shared<TTargetTransfer::TTransferConfig>(path.first, path.second, Config));
            }
        } else {
            YDB_LOG_ERROR("Describe topic failed",
                {"path", path.first},
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});

            if (IsRetryableError(result, Backoff)) {
                return RetryDescribe(*it);
            } else {
                Failed.emplace_back(path.first, result);
            }
        }

        Pending.erase(it);
        MaybeReply();
    }

    bool CheckTopic(const NYdb::NTopic::TTopicDescription& description, TString& error) const {
        if (Config.GetConsistencySettings().GetLevelCase() != NKikimrReplication::TConsistencySettings::kRow) {
            error = "Topic replication supports row consistency only";
            return false;
        }
        if (description.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
            != NYdb::NTopic::EAutoPartitioningStrategy::Disabled)
        {
            error = "Topic replication requires fixed partitions";
            return false;
        }

        const auto& partitions = description.GetPartitions();
        if (partitions.empty()) {
            error = "Source topic has no partitions";
            return false;
        }
        const auto invalidPartition = FindIf(partitions, [&](const auto& partition) {
            return !partition.GetActive() || partition.GetPartitionId() >= partitions.size()
                || !partition.GetParentPartitionIds().empty() || !partition.GetChildPartitionIds().empty();
        });
        if (invalidPartition != partitions.end()) {
            error = "Topic replication requires unsplit contiguous partitions";
            return false;
        }
        return true;
    }

    void RejectTarget(ui32 idx, const TString& error) {
        NYdb::NIssue::TIssues issues;
        issues.AddIssue(error);
        Failed.emplace_back(Paths.at(idx).first, NYdb::TStatus(NYdb::EStatus::UNSUPPORTED, std::move(issues)));
        Pending.erase(idx);
        MaybeReply();
    }

    void MaybeReply() {
        if (Pending || Listings) {
            return;
        }

        if (Failed) {
            Send(Parent, new TEvPrivate::TEvDiscoveryTargetsResult(ReplicationId, std::move(Failed)));
        } else {
            Send(Parent, new TEvPrivate::TEvDiscoveryTargetsResult(ReplicationId, std::move(ToAdd), {}));
        }

        PassAway();
    }

    void ListDirectory(const std::pair<TString, TString>& path) {
        auto res = Listings.emplace(NextListingId++, path);
        Y_ABORT_UNLESS(res.second);
        ListDirectory(res.first->first);
    }

    void ListDirectory(ui64 listingId) {
        auto it = Listings.find(listingId);
        Y_ABORT_UNLESS(it != Listings.end());
        Send(YdbProxy, new TEvYdbProxy::TEvListDirectoryRequest(it->second.first, {}), 0, it->first);
    }

    void Handle(TEvYdbProxy::TEvListDirectoryResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        auto it = Listings.find(ev->Cookie);
        if (it == Listings.end()) {
            YDB_LOG_WARN("Unknown listing response",
                {"cookie", ev->Cookie});
            return;
        }

        const auto& path = it->second;
        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            YDB_LOG_DEBUG("Listing succeeded",
                {"path", path.first});

            for (const auto& child : result.GetChildren()) {
                switch (child.Type) {
                case NYdb::NScheme::ESchemeEntryType::SubDomain:
                case NYdb::NScheme::ESchemeEntryType::Directory:
                    if (child.Type != NYdb::NScheme::ESchemeEntryType::Directory
                        || !NNaming::IsInternalName(child.Name))
                    {
                        ListDirectory(std::make_pair(
                            path.first  + '/' + child.Name,
                            path.second + '/' + child.Name));
                    }
                    break;
                case NYdb::NScheme::ESchemeEntryType::Table:
                    Paths.emplace_back(
                        path.first  + '/' + child.Name,
                        path.second + '/' + child.Name);
                    DescribeTable(Paths.size() - 1);
                    break;
                default:
                    break;
                }
            }
        } else {
            YDB_LOG_ERROR("Listing failed",
                {"path", path.first},
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});

            if (IsRetryableError(result, Backoff)) {
                return RetryListing(it->first);
            } else {
                Failed.emplace_back(path.first, result);
            }
        }

        Listings.erase(it);
        MaybeReply();
    }

    void ScheduleRetry() {
        if (DescribeRetries.empty() && ListingRetries.empty()) {
            Schedule(Backoff.Next(), new TEvents::TEvWakeup);
        }
    }

    void RetryDescribe(ui32 idx) {
        ScheduleRetry();
        DescribeRetries.insert(idx);
    }

    void RetryListing(ui64 id) {
        ScheduleRetry();
        ListingRetries.insert(id);
    }

    void Retry() {
        for (const ui32 idx : DescribeRetries) {
            DescribePath(idx);
        }

        for (const ui64 id : ListingRetries) {
            ListDirectory(id);
        }

        DescribeRetries.clear();
        ListingRetries.clear();
    }

    bool IsReplication() const {
        return Config.HasSpecific();
    }

    bool IsTransfer() const {
        return Config.HasTransferSpecific();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DISCOVERER;
    }

    explicit TTargetDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
        const NKikimrReplication::TReplicationConfig& config)
        : Parent(parent)
        , ReplicationId(rid)
        , YdbProxy(proxy)
        , Config(config)
        , LogPrefix(CreateActorLogPrefix("TargetDiscoverer", ReplicationId))
        , Backoff(5)
    {
        if (Config.HasSpecific()) {
            for (const auto& target : Config.GetSpecific().GetTargets()) {
                Paths.emplace_back(target.GetSrcPath(), target.GetDstPath());
            }
        } else if (Config.HasTransferSpecific()) {
            const auto& target = Config.GetTransferSpecific().GetTarget();
            Paths.emplace_back(target.GetSrcPath(), target.GetDstPath());
        } else {
            Y_ABORT("Unsupported");
        }
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        for (ui32 i = 0; i < Paths.size(); ++i) {
            DescribePath(i);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWork"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribePathResponse, Handle);
            hFunc(TEvYdbProxy::TEvListDirectoryResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            sFunc(TEvents::TEvWakeup, Retry);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TActorId YdbProxy;
    const NKikimrReplication::TReplicationConfig Config;
    TVector<std::pair<TString, TString>> Paths;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    ui64 NextListingId = 1;
    THashMap<ui64, std::pair<TString, TString>> Listings;

    THashSet<ui32> Pending;
    THashSet<ui32> TopicParents;
    THashSet<ui32> DescribeRetries;
    THashSet<ui64> ListingRetries;
    TVector<TEvPrivate::TEvDiscoveryTargetsResult::TAddEntry> ToAdd;
    TVector<TEvPrivate::TEvDiscoveryTargetsResult::TFailedEntry> Failed;

    TBackoff Backoff;

}; // TTargetDiscoverer

IActor* CreateTargetDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
    const NKikimrReplication::TReplicationConfig& config)
{
    return new TTargetDiscoverer(parent, rid, proxy, config);
}

}
