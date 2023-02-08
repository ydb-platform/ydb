#include "discoverer.h"
#include "logging.h"
#include "private_events.h"
#include "util.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NReplication::NController {

class TDiscoverer: public TActorBootstrapped<TDiscoverer> {
    void DescribePath(ui32 idx) {
        Y_VERIFY(idx < Paths.size());
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

        Y_VERIFY(*it < Paths.size());
        const auto& path = Paths.at(*it);

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe succeeded"
                << ": path# " << path.first);

            auto entry = result.GetEntry();
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
                << ": path# " << path.first);

            if (IsRetryableError(result)) {
                return Retry(*it);
            } else {
                Failed.emplace_back(path.first, result);
            }
        }

        Pending.erase(it);
        if (Pending) {
            return;
        }

        if (Failed) {
            Send(Parent, new TEvPrivate::TEvDiscoveryResult(ReplicationId, std::move(Failed)));
        } else {
            Send(Parent, new TEvPrivate::TEvDiscoveryResult(ReplicationId, std::move(ToAdd), {}));
        }

        PassAway();
    }

    void Retry(ui32 idx) {
        if (ToRetry.empty()) {
            Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
        }

        ToRetry.insert(idx);
    }

    void Retry() {
        for (const ui32 idx : ToRetry) {
            DescribePath(idx);
        }

        ToRetry.clear();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_DISCOVERER;
    }

    explicit TDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy, TVector<std::pair<TString, TString>>&& paths)
        : Parent(parent)
        , ReplicationId(rid)
        , YdbProxy(proxy)
        , Paths(std::move(paths))
        , LogPrefix("Discoverer", ReplicationId)
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
            sFunc(TEvents::TEvWakeup, Retry);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TActorId YdbProxy;
    const TVector<std::pair<TString, TString>> Paths;
    const TActorLogPrefix LogPrefix;

    THashSet<ui32> Pending;
    THashSet<ui32> ToRetry;
    TVector<TEvPrivate::TEvDiscoveryResult::TAddEntry> ToAdd;
    TVector<TEvPrivate::TEvDiscoveryResult::TFailedEntry> Failed;

}; // TDiscoverer

IActor* CreateDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
    TVector<std::pair<TString, TString>>&& specificPaths)
{
    return new TDiscoverer(parent, rid, proxy, std::move(specificPaths));
}

}
