#include "logging.h"
#include "private_events.h"
#include "target_discoverer.h"
#include "util.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

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
            LOG_W("Unknown describe response"
                << ": cookie# " << ev->Cookie);
            return;
        }

        Y_ABORT_UNLESS(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe succeeded"
                << ": path# " << path.first);

            auto entry = result.GetEntry();
            switch (entry.Type) {
            case NYdb::NScheme::ESchemeEntryType::SubDomain:
            case NYdb::NScheme::ESchemeEntryType::Directory:
                Pending.erase(it);
                return ListDirectory(path);
            default:
                break;
            }

            entry.Name = path.first; // replace by full path

            if (const auto kind = TryTargetKindFromEntryType(entry.Type)) {
                LOG_I("Add target"
                    << ": path# " << path.first
                    << ", kind# " << kind);
                ToAdd.emplace_back(std::move(entry), path.second);
            } else {
                LOG_W("Unsupported entry type"
                    << ": path# " << path.first
                    << ", type# " << entry.Type);

                NYql::TIssues issues;
                issues.AddIssue(TStringBuilder() << "Unsupported entry type: " << entry.Type);
                Failed.emplace_back(path.first, NYdb::TStatus(NYdb::EStatus::UNSUPPORTED, std::move(issues)));
            }
        } else {
            LOG_E("Describe failed"
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

        return entry.Name.StartsWith("~")
            || entry.Name.StartsWith(".sys")
            || entry.Name.StartsWith(".metadata");
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
                default:
                    if (TryTargetKindFromEntryType(child.Type)) {
                        Paths.emplace_back(
                            path.first  + '/' + child.Name,
                            path.second + '/' + child.Name);
                        DescribePath(Paths.size() - 1);
                    }
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

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DISCOVERER;
    }

    explicit TTargetDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy, TVector<std::pair<TString, TString>>&& paths)
        : Parent(parent)
        , ReplicationId(rid)
        , YdbProxy(proxy)
        , Paths(std::move(paths))
        , LogPrefix("TargetDiscoverer", ReplicationId)
    {
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
            sFunc(TEvents::TEvWakeup, Retry);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TActorId YdbProxy;
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
    TVector<std::pair<TString, TString>>&& specificPaths)
{
    return new TTargetDiscoverer(parent, rid, proxy, std::move(specificPaths));
}

}
