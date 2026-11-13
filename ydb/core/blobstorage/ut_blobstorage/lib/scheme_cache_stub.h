#pragma once

#include "env.h"

#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

// test environment has no SchemeCache, while BS_CONTROLLER resolves database of the decommitted group through it;
// this stub describes the root domain containing provided storage pools
class TSchemeCacheStub : public TActor<TSchemeCacheStub> {
    const NKikimrSubDomains::TDomainDescription Description;

public:
    TSchemeCacheStub(NKikimrSubDomains::TDomainDescription description)
        : TActor(&TThis::StateFunc)
        , Description(std::move(description))
    {}

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr ev) {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = ev->Get()->Request.Release();
        for (auto& entry : request->ResultSet) {
            entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
            entry.DomainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(Description);
            auto domain = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TDomainDescription>();
            domain->Description = Description;
            entry.DomainDescription = domain;
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request), 0, ev->Cookie);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle)
    )
};

inline void SetupSchemeCacheStub(TEnvironmentSetup& env, std::vector<TString> pools = {}) {
    NKikimrSubDomains::TDomainDescription description;
    auto *key = description.MutableDomainKey();
    key->SetSchemeShard(env.Runtime->GetDomainsInfo()->GetDomain()->SchemeRoot);
    key->SetPathId(1);
    pools.push_back(env.StoragePoolName);
    for (const TString& name : pools) {
        auto *pool = description.AddStoragePools();
        pool->SetName(name);
        pool->SetKind(name);
    }

    for (ui32 nodeId : env.Runtime->GetNodes()) {
        env.Runtime->RegisterService(MakeSchemeCacheID(), env.Runtime->Register(new TSchemeCacheStub(description),
            nodeId));
    }
}
