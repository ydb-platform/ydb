#include "domain_info.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

namespace {

constexpr double EPS = 0.001;
constexpr double PERCENT_EPS = 0.01; // 1%

template <typename TIt>
ui32 CalculateRecommendedNodes(TIt windowBegin, TIt windowEnd, size_t readyNodes, double target) {
    double maxOnWindow = *std::max_element(windowBegin, windowEnd);
    double ratio = maxOnWindow / target;

    double recommendedNodes = readyNodes * ratio;
    if (recommendedNodes - std::floor(recommendedNodes) < EPS) {
        return std::floor(recommendedNodes);
    }

    return std::ceil(recommendedNodes);
}

} // anonymous

ENodeSelectionPolicy TDomainInfo::GetNodeSelectionPolicy() const {
    if (ServerlessComputeResourcesMode.Empty()) {
        return ENodeSelectionPolicy::Default;
    }

    switch (*ServerlessComputeResourcesMode) {
        case NKikimrSubDomains::EServerlessComputeResourcesModeExclusive:
            return ENodeSelectionPolicy::PreferObjectDomain;
        case NKikimrSubDomains::EServerlessComputeResourcesModeShared:
            return ENodeSelectionPolicy::Default;
        default:
            return ENodeSelectionPolicy::Default;
    }
}

TScaleRecommenderPolicy::TScaleRecommenderPolicy(ui64 hiveId, bool dryRun)
    : HiveId(hiveId)
    , DryRun(dryRun)
{}

TString TScaleRecommenderPolicy::GetLogPrefix() const {
    TStringBuilder logPrefix = TStringBuilder() << "HIVE#" << HiveId << " ";
    if (DryRun) {
        logPrefix << "[DryRun] ";
    }
    return logPrefix;
}

TTargetTrackingPolicy::TTargetTrackingPolicy(double target, const std::deque<double>& usageHistory, ui64 hiveId, bool dryRun)
    : TScaleRecommenderPolicy(hiveId, dryRun)
    , TargetUsage(target)
    , UsageHistory(usageHistory)
{}

TString TTargetTrackingPolicy::GetLogPrefix() const {
    return TStringBuilder() << TScaleRecommenderPolicy::GetLogPrefix() << "[TargetTracking] ";
}

ui32 TTargetTrackingPolicy::MakeScaleRecommendation(ui32 readyNodesCount, const NKikimrConfig::THiveConfig& config) const {
    ui32 recommendedNodes = readyNodesCount;

    if (UsageHistory.size() >= config.GetScaleInWindowSize()) {
        auto scaleInWindowBegin = UsageHistory.end() - config.GetScaleInWindowSize();
        auto scaleInWindowEnd = UsageHistory.end();
        double usageBottomThreshold = TargetUsage - config.GetTargetTrackingCPUMargin();

        BLOG_TRACE("[MSR] Scale in window: [" << JoinRange(", ", scaleInWindowBegin, scaleInWindowEnd)
                   << "], bottom threshold: " << usageBottomThreshold);
        bool needScaleIn = std::all_of(
            scaleInWindowBegin,
            scaleInWindowEnd,
            [usageBottomThreshold](double value){ return value <= usageBottomThreshold - PERCENT_EPS; }
        );

        if (needScaleIn) {
            recommendedNodes = CalculateRecommendedNodes(
                scaleInWindowBegin,
                scaleInWindowEnd,
                readyNodesCount,
                TargetUsage
            );
            BLOG_TRACE("[MSR] Need scale in, rounded recommended nodes: " << recommendedNodes);
        }
    } else {
        BLOG_TRACE("[MSR] Not enough history for scale in");
    }

    if (UsageHistory.size() >= config.GetScaleOutWindowSize()) {
        auto scaleOutWindowBegin = UsageHistory.end() - config.GetScaleOutWindowSize();
        auto scaleOutWindowEnd = UsageHistory.end();

        BLOG_TRACE("[MSR] Scale out window: [" << JoinRange(", ", scaleOutWindowBegin, scaleOutWindowEnd)
                   << "], target: " << TargetUsage);
        bool needScaleOut = std::all_of(
            scaleOutWindowBegin,
            scaleOutWindowEnd,
            [this](double value){ return value >= TargetUsage + PERCENT_EPS; }
        );

        if (needScaleOut) {
            recommendedNodes = CalculateRecommendedNodes(
                scaleOutWindowBegin,
                scaleOutWindowEnd,
                readyNodesCount,
                TargetUsage
            );
            BLOG_TRACE("[MSR] Need scale out, rounded recommended nodes: " << recommendedNodes);
        }
    } else {
        BLOG_TRACE("[MSR] Not enough history for scale out");
    }

    return std::max(recommendedNodes, 1u);
}

void TDomainInfo::SetScaleRecommenderPolicies(const NKikimrHive::TScaleRecommenderPolicies& policies) {
    using enum NKikimrHive::TScaleRecommenderPolicies_TScaleRecommenderPolicy::PolicyCase;
    using enum NKikimrHive::TScaleRecommenderPolicies_TScaleRecommenderPolicy_TTargetTrackingPolicy::TargetCase;

    ScaleRecommenderPolicies.clear();
    for (const auto& policy : policies.GetPolicies()) {
        switch (policy.GetPolicyCase()) {
            case kTargetTrackingPolicy: {
                const auto& targetTracking = policy.GetTargetTrackingPolicy();
                switch (targetTracking.GetTargetCase()) {
                    case kAverageCpuUtilizationPercent: {
                        ui32 target = targetTracking.GetAverageCpuUtilizationPercent();
                        auto convertedPolicy = std::make_shared<TTargetTrackingPolicy>(target / 100., AvgCpuUsageHistory, HiveId);
                        ScaleRecommenderPolicies.push_back(convertedPolicy);
                        break;
                    }
                    case TARGET_NOT_SET:
                        break;
                }
                break;
            }
            case POLICY_NOT_SET:
                break;
        }
    }
}

} // NHive
} // NKikimr
