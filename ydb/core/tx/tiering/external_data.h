#pragma once
#include "snapshot.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TSnapshotConstructor: public NMetadata::NFetcher::TSnapshotsFetcher<TConfigsSnapshot> {
private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TBaseActor = TActor<TSnapshotConstructor>;
    using ISnapshot = NMetadata::NFetcher::ISnapshot;
protected:
    virtual std::vector<NMetadata::IClassBehaviour::TPtr> DoGetManagers() const override;
public:
    virtual void EnrichSnapshotData(ISnapshot::TPtr original, NMetadata::NFetcher::ISnapshotAcceptorController::TPtr controller) const override;

    TSnapshotConstructor();
};

}
