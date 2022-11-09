#pragma once
#include "rule.h"
#include "tier_config.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TActorOwnerEnrich: public TActorBootstrapped<TActorOwnerEnrich> {
private:
    using TBase = TActorBootstrapped<TActorOwnerEnrich>;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    const TString OwnerPath;
    NMetadataInitializer::IController::TPtr Controller;
    const TVector<TString> TableNames;
private:
    TVector<NMetadataInitializer::ITableModifier::TPtr> BuildModifiers(const NSchemeCache::TSchemeCacheNavigate::TEntry& ownerEntry) const;

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TIERING_OWNER_ENRICH;
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                break;
        }
    }

    TActorOwnerEnrich(const TString& ownerPath, NMetadataInitializer::IController::TPtr controller, const TVector<TString>& tableNames)
        : OwnerPath(ownerPath)
        , Controller(controller)
        , TableNames(tableNames)
    {

    }

    void Bootstrap();
};

}
