#pragma once
#include "rule.h"
#include "tier_config.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTablesDecoderCache {
private:
    using TRemapper = TMap<TString, ui64>;
    YDB_ACCESSOR_DEF(TRemapper, Remapper);
public:
    using TPtr = std::shared_ptr<TTablesDecoderCache>;
};

class TActorSnapshotEnrich: public TActorBootstrapped<TActorSnapshotEnrich> {
private:
    using TBase = TActorBootstrapped<TActorSnapshotEnrich>;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    NMetadataProvider::ISnapshot::TPtr Original;
    NMetadataProvider::ISnapshotAcceptorController::TPtr Controller;
    TTablesDecoderCache::TPtr Cache;
private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TIERING_SNAPSHOT_ENRICH;
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                break;
        }
    }

    TActorSnapshotEnrich(NMetadataProvider::ISnapshot::TPtr original,
        NMetadataProvider::ISnapshotAcceptorController::TPtr controller, TTablesDecoderCache::TPtr cache)
        : Original(original)
        , Controller(controller)
        , Cache(cache)
    {

    }

    void Bootstrap();
};

}
