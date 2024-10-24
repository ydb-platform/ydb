#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/tier/snapshot.h>

#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierSnapshotConstructor: public NMetadata::NFetcher::TSnapshotsFetcher<TTiersSnapshot> {
private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TBaseActor = TActor<TTierSnapshotConstructor>;
    using ISnapshot = NMetadata::NFetcher::ISnapshot;
protected:
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override;
public:
    virtual void EnrichSnapshotData(ISnapshot::TPtr original, NMetadata::NFetcher::ISnapshotAcceptorController::TPtr controller) const override;

    TTierSnapshotConstructor();
};
}
