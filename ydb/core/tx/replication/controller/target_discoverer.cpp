#include "logging.h"
#include "private_events.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "target_transfer.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NReplication::NController {

class TTargetDiscoverer: public TActorBootstrapped<TTargetDiscoverer> {
    void DescribePath(ui32 idx) {
        Y_ABORT_UNLESS(idx < Paths.size());
        Send(YdbProxy, new TEvYdbProxy::TEvDescribePathRequest(Paths.at(idx).first, {}), 0, idx);
        Pending.insert(idx);
    }

    void Handle(TEvYdbProxy::TEvDescribePathResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            LOG_W("Unknown describe path response"
                << ": cookie# " << ev->Cookie);
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe path succeeded"
                << ": path# " << path.first);

            const auto& entry = result.GetEntry();
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
                if (IsTransfer()) {
                    return DescribeTopic(ev->Cookie);
                }
                break;
            default:
                break;
            }

            LOG_W("Unsupported entry type"
                << ": path# " << path.first
                << ", type# " << entry.Type);

            NYdb::NIssue::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Unsupported entry type: " << entry.Type);
            Failed.emplace_back(path.first, NYdb::TStatus(NYdb::EStatus::UNSUPPORTED, std::move(issues)));
        } else {
            LOG_E("Describe path failed"
                << ": path# " << path.first
                << ", status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString());

            if (IsRetryableError(result)) {
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
        LOG_T("Handle " << ev->Get()->ToString());

        auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            LOG_W("Unknown describe table response"
                << ": cookie# " << ev->Cookie);
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe table succeeded"
                << ": path# " << path.first);

            const auto& target = ToAdd.emplace_back(TReplication::ETargetKind::Table,
                std::make_shared<TTargetTable::TTableConfig>(path.first, path.second));
            LOG_I("Add target"
                << ": srcPath# " << target.Config->GetSrcPath()
                << ", dstPath# " << target.Config->GetDstPath()
                << ", kind# " << target.Kind);

            for (const auto& index : result.GetTableDescription().GetIndexDescriptions()) {
                switch (index.GetIndexType()) {
                case NYdb::NTable::EIndexType::GlobalSync:
                case NYdb::NTable::EIndexType::GlobalUnique:
                    break;
                default:
                    continue;
                }

                const auto& target = ToAdd.emplace_back(
                    TReplication::ETargetKind::IndexTable,
                    std::make_shared<TTargetIndexTable::TIndexTableConfig>(
                        CanonizePath(ChildPath(SplitPath(path.first), TString{index.GetIndexName()})),
                        CanonizePath(ChildPath(SplitPath(path.second), {TString{index.GetIndexName()}, "indexImplTable"}))
                    ));
                LOG_I("Add target"
                    << ": srcPath# " << target.Config->GetSrcPath()
                    << ", dstPath# " << target.Config->GetDstPath()
                    << ", kind# " << target.Kind);
            }
        } else {
            LOG_E("Describe table failed"
                << ": path# " << path.first
                << ", status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString());

            if (IsRetryableError(result)) {
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
        LOG_T("Handle " << ev->Get()->ToString());

        auto it = Pending.find(ev->Cookie);
        if (it == Pending.end()) {
            LOG_W("Unknown describe topic response"
                << ": cookie# " << ev->Cookie);
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe topic succeeded"
                << ": path# " << path.first);

            const auto& targetConf = Config.GetTransferSpecific().GetTarget();

            const auto& target = ToAdd.emplace_back(TReplication::ETargetKind::Transfer,
                std::make_shared<TTargetTransfer::TTransferConfig>(path.first, path.second, targetConf.GetTransformLambda(),
                    Config.GetTransferSpecific().GetRunAsUser(), targetConf.GetDirectoryPath()));
            LOG_I("Add target"
                << ": srcPath# " << target.Config->GetSrcPath()
                << ", dstPath# " << target.Config->GetDstPath()
                << ", kind# " << target.Kind);
        } else {
            LOG_E("Describe topic failed"
                << ": path# " << path.first
                << ", status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString());

            if (IsRetryableError(result)) {
                return RetryDescribe(*it);
            } else {
                Failed.emplace_back(path.first, result);
            }
        }

        Pending.erase(it);
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

    static bool IsSystemObject(const NYdb::NScheme::TSchemeEntry& entry) {
        if (entry.Type != NYdb::NScheme::ESchemeEntryType::Directory) {
            return false;
        }

        return entry.Name.starts_with("~")
            || entry.Name.starts_with(".sys")
            || entry.Name.starts_with(".metadata")
            || entry.Name.starts_with("export-");
    }

    void Handle(TEvYdbProxy::TEvListDirectoryResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        auto it = Listings.find(ev->Cookie);
        if (it == Listings.end()) {
            LOG_W("Unknown listing response"
                << ": cookie# " << ev->Cookie);
            return;
        }

        const auto& path = it->second;
        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Listing succeeded"
                << ": path# " << path.first);

            for (const auto& child : result.GetChildren()) {
                switch (child.Type) {
                case NYdb::NScheme::ESchemeEntryType::SubDomain:
                case NYdb::NScheme::ESchemeEntryType::Directory:
                    if (!IsSystemObject(child)) {
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
            LOG_E("Listing failed"
                << ": path# " << path.first
                << ", status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString());

            if (IsRetryableError(result)) {
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
            Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
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
        , LogPrefix("TargetDiscoverer", ReplicationId)
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
        for (ui32 i = 0; i < Paths.size(); ++i) {
            DescribePath(i);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
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
    const TActorLogPrefix LogPrefix;

    ui64 NextListingId = 1;
    THashMap<ui64, std::pair<TString, TString>> Listings;

    THashSet<ui32> Pending;
    THashSet<ui32> DescribeRetries;
    THashSet<ui64> ListingRetries;
    TVector<TEvPrivate::TEvDiscoveryTargetsResult::TAddEntry> ToAdd;
    TVector<TEvPrivate::TEvDiscoveryTargetsResult::TFailedEntry> Failed;

}; // TTargetDiscoverer

IActor* CreateTargetDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
    const NKikimrReplication::TReplicationConfig& config)
{
    return new TTargetDiscoverer(parent, rid, proxy, config);
}

}
