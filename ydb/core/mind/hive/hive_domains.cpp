#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

void TDomainsView::RegisterNode(const TNodeInfo& node) {
    for (auto &domainKey: node.ServicedDomains) {
        ++TotalCount[domainKey];
    }
}

void TDomainsView::DeregisterNode(const TNodeInfo& node) {
    for (auto &domainKey: node.ServicedDomains) {
         BLOG_TRACE("Node(" << node.Id << ")"
                    << " DeregisterInDomains (" << domainKey << ") : " << TotalCount[domainKey] << " -> " << TotalCount[domainKey] - 1);
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
    request->ResultSet.emplace_back();
    auto& entry = request->ResultSet.back();
    entry.TableId = TTableId(domain.first, domain.second);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    BLOG_D("Resolving domain " << entry.TableId);
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
            if (entry.DomainInfo) {
                Domains[key].HiveId = entry.DomainInfo->Params.GetHive();
                if (entry.DomainInfo->ServerlessComputeResourcesMode && Domains[key].ServerlessComputeResourcesMode.Empty()) {
                    Domains[key].ServerlessComputeResourcesMode = entry.DomainInfo->ServerlessComputeResourcesMode;
                }
            }
            BLOG_D("Received NavigateKeySetResult for domain " << entry.TableId << " with path " << path);
            Execute(CreateUpdateDomain(key));
        } else {
            BLOG_W("Received NavigateKeySetResult for domain " << entry.TableId << " with status " << entry.Status);
        }
    } else {
        BLOG_W("Received empty NavigateKeySetResult");
    }
}

void THive::Handle(TEvHive::TEvUpdateDomain::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvUpdateDomain(" << ev->Get()->Record.ShortDebugString() << ")");
    const TSubDomainKey subdomainKey(ev->Get()->Record.GetDomainKey());
    TDomainInfo& domainInfo = Domains[subdomainKey];
    if (ev->Get()->Record.GetServerlessComputeResourcesMode() != NKikimrSubDomains::SERVERLESS_COMPUTE_RESOURCES_MODE_UNSPECIFIED) {
        domainInfo.ServerlessComputeResourcesMode = ev->Get()->Record.GetServerlessComputeResourcesMode();
    } else {
        domainInfo.ServerlessComputeResourcesMode.Clear();
    }
    Execute(CreateUpdateDomain(subdomainKey, std::move(ev)));
}

TString THive::GetDomainName(TSubDomainKey domain) {
    auto itDomain = Domains.find(domain);
    if (itDomain != Domains.end()) {
        if (!itDomain->second.Path.empty()) {
            return itDomain->second.Path;
        }
    } else {
        SeenDomain(domain);
    }
    if (domain == TSubDomainKey()) {
        return "<empty-subdomain-key>";
    }
    return TStringBuilder() << domain;
}

} // NHive
} // NKikimr
