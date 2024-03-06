#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor.h>
#include <util/generic/ptr.h>

namespace NKikimr::NGRpcProxy::V1 {

inline NActors::TActorId GetPQSchemaServiceActorID() {
    return NActors::TActorId(0, "PQSchmSvc");
}

struct TClustersCfg : public TThrRefBase {
    TClustersCfg()
        : LocalCluster("")
    {}
    TString LocalCluster;
    TVector<TString> Clusters;
};

class IClustersCfgProvider {
public:
    virtual ~IClustersCfgProvider() = default;
    virtual TIntrusiveConstPtr<TClustersCfg> GetCfg() const = 0;
};

NActors::IActor* CreatePQSchemaService(IClustersCfgProvider** cfgProvider);
}
