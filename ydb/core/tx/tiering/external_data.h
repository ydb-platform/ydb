#pragma once
#include "snapshot.h"
#include "snapshot_enrich.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TSnapshotConstructor: public NMetadataProvider::TGenericSnapshotParser<TConfigsSnapshot> {
private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TBaseActor = TActor<TSnapshotConstructor>;
    using ISnapshot = NMetadataProvider::ISnapshot;
    TVector<TString> Tables;
    TString TablePath;
    const TString OwnerPath;
    mutable std::shared_ptr<TTablesDecoderCache> TablesDecoder;
protected:
    virtual const TVector<TString>& DoGetTables() const override {
        return Tables;
    }
    virtual void DoPrepare(NMetadataInitializer::IController::TPtr controller) const override;
public:
    virtual void EnrichSnapshotData(ISnapshot::TPtr original, NMetadataProvider::ISnapshotAcceptorController::TPtr controller) const override;

    TSnapshotConstructor(const TString& ownerPath);
};

}
