#pragma once

#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <library/cpp/sliding_window/sliding_window.h>

#include <util/generic/fwd.h>



namespace NKikimr::NPQ {

class IAutopartitioningManager {
public:
    virtual ~IAutopartitioningManager() = default;

    virtual void OnWrite(const TString& sourceId, ui64 size) = 0;

    virtual NKikimrPQ::EScaleStatus GetScaleStatus(const NActors::TActorContext& ctx) = 0;
    virtual std::optional<TString> SplitBoundary() = 0;

    virtual void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) = 0;
};

IAutopartitioningManager* CreateAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, bool supportive);

}
