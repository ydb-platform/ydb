#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

void TDomainsView::RegisterNode(const TNodeInfo& node) {
    for (auto &domainKey: node.ServicedDomains) {
        ++TotalCount[domainKey];
    }
}

void TDomainsView::DeregisterNode(const TNodeInfo& node) {
    for (auto &domainKey: node.ServicedDomains) {
         YDB_LOG_TRACE("Node( DeregisterInDomains ( ->",
             {"GetLogPrefix", GetLogPrefix()},
             {"Id", node.Id},
             {"domainKey", domainKey},
             {"#_TotalCount[domainKey]", TotalCount[domainKey]},
             {"#_TotalCount[domainKey] - 1", TotalCount[domainKey] - 1});
         Y_ABORT_UNLESS(TotalCount[domainKey], "try decrement empty counter for DomainKey %s", ToString(domainKey).c_str());
        --TotalCount[domainKey];
    }
}

bool THive::SeenDomain(TSubDomainKey domain) {
    auto emResult = Domains.emplace(domain, TDomainInfo());
    if (emResult.second || emResult.first->second.Path.empty()) {
        emResult.first->second.Path = TStringBuilder() << domain;
        ResolveDomain(domain);
        return false;
    }
    return true;
}

void THive::ResolveDomain(TSubDomainKey domain) {
    THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = RootDomainName;

    request->ResultSet.emplace_back();
    auto& entry = request->ResultSet.back();
    entry.TableId = TTableId(domain.first, domain.second);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    YDB_LOG_DEBUG("Resolving domain",
        {"GetLogPrefix", GetLogPrefix()},
        {"TableId", entry.TableId});
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void THive::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    NSchemeCache::TSchemeCacheNavigate* request = ev->Get()->Request.Get();
    if (!request->ResultSet.empty()) {
        auto& entry = request->ResultSet.front();
        if (entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            TSubDomainKey key(entry.TableId.PathId.OwnerId, entry.TableId.PathId.LocalPathId);
            TString path = CanonizePath(entry.Path);
            Domains[key].Path = path;
            if (entry.DomainInfo && entry.DomainInfo->Params.HasHive()) {
                Domains[key].HiveId = entry.DomainInfo->Params.GetHive();
            }
            YDB_LOG_DEBUG("Received NavigateKeySetResult for domain with path",
                {"GetLogPrefix", GetLogPrefix()},
                {"TableId", entry.TableId},
                {"path", path});
            Execute(CreateUpdateDomain(key));
        } else {
            YDB_LOG_WARN("Received NavigateKeySetResult for domain with status",
                {"GetLogPrefix", GetLogPrefix()},
                {"TableId", entry.TableId},
                {"Status", entry.Status});
        }
    } else {
        YDB_LOG_WARN("Received empty NavigateKeySetResult",
            {"GetLogPrefix", GetLogPrefix()});
    }
}

void THive::Handle(TEvHive::TEvUpdateDomain::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvHive::TEvUpdateDomain(",
        {"GetLogPrefix", GetLogPrefix()},
        {"ShortDebugString", ev->Get()->Record.ShortDebugString()});
    const TSubDomainKey subdomainKey(ev->Get()->Record.GetDomainKey());
    TDomainInfo& domainInfo = Domains[subdomainKey];
    if (ev->Get()->Record.HasServerlessComputeResourcesMode()) {
        domainInfo.ServerlessComputeResourcesMode = ev->Get()->Record.GetServerlessComputeResourcesMode();
    } else {
        domainInfo.ServerlessComputeResourcesMode.Clear();
    }
    Execute(CreateUpdateDomain(subdomainKey, std::move(ev)));
}

TString THive::GetDomainName(TSubDomainKey domain) {
    if (domain == TSubDomainKey()) {
        return "<empty-subdomain-key>";
    }
    SeenDomain(domain);
    return Domains.at(domain).Path;
}

} // NHive
} // NKikimr
