#pragma once

#include "hive.h"

namespace NKikimr {
namespace NHive {

struct TScaleRecommendation {
    ui64 Nodes = 0;
    TInstant Timestamp;
};

enum class ENodeSelectionPolicy : ui32 {
    Default,
    PreferObjectDomain,
};

class TScaleRecommenderPolicy {
public:
    TScaleRecommenderPolicy(ui64 hiveId, bool dryRun);
    virtual ~TScaleRecommenderPolicy() = default;
    virtual ui32 MakeScaleRecommendation(ui32 readyNodes, const NKikimrConfig::THiveConfig& config) const = 0;

    virtual TString GetLogPrefix() const;
private:
    ui64 HiveId;
    bool DryRun;
};

class TTargetTrackingPolicy : public TScaleRecommenderPolicy {
public:
    TTargetTrackingPolicy(double target, const std::deque<double>& usageHistory, ui64 hiveId = 0, bool dryRun = false);
    ui32 MakeScaleRecommendation(ui32 readyNodesCount, const NKikimrConfig::THiveConfig& config) const override;

    virtual TString GetLogPrefix() const override;
private:
    double TargetUsage;
    const std::deque<double>& UsageHistory;
};

struct TDomainInfo {
    TString Path;
    TTabletId HiveId = 0;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;
    bool Stopped = false;

    ui64 TabletsTotal = 0;
    ui64 TabletsAlive = 0;
    ui64 TabletsAliveInObjectDomain = 0;
    
    std::deque<double> AvgCpuUsageHistory;
    TMaybeFail<TScaleRecommendation> LastScaleRecommendation;
    TVector<std::shared_ptr<TScaleRecommenderPolicy>> ScaleRecommenderPolicies;

    void SetScaleRecommenderPolicies(const NKikimrHive::TScaleRecommenderPolicies& policies);

    ENodeSelectionPolicy GetNodeSelectionPolicy() const;
};

} // NHive
} // NKikimr
