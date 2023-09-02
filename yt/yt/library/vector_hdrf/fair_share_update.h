#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/public.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>
#include <yt/yt/library/vector_hdrf/resource_volume.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateExecutor;
struct TFairShareUpdateContext;

class TElement;
class TCompositeElement;
class TPool;
class TRootElement;
class TOperationElement;

////////////////////////////////////////////////////////////////////////////////

struct TDetailedFairShare
{
    TResourceVector StrongGuarantee = {};
    TResourceVector IntegralGuarantee = {};
    TResourceVector WeightProportional = {};
    TResourceVector Total = {};
};

TString ToString(const TDetailedFairShare& detailedFairShare);

void FormatValue(TStringBuilderBase* builder, const TDetailedFairShare& detailedFairShare, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

struct TIntegralResourcesState
{
    TResourceVolume AccumulatedVolume;
    double LastShareRatio = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    EJobResourceType DominantResource = EJobResourceType::Cpu;

    TDetailedFairShare FairShare;
    TResourceVector UsageShare;
    TResourceVector DemandShare;
    TResourceVector LimitsShare;
    TResourceVector StrongGuaranteeShare;
    TResourceVector ProposedIntegralShare;
    TResourceVector PromisedFairShare;
    TResourceVector EstimatedGuaranteeShare;

    TResourceVolume VolumeOverflow;
    TResourceVolume AcceptableVolume;
    TResourceVolume AcceptedFreeVolume;
    TResourceVolume ChildrenVolumeOverflow;

    TJobResources EffectiveStrongGuaranteeResources;

    double BurstRatio = 0.0;
    double TotalBurstRatio = 0.0;
    double ResourceFlowRatio = 0.0;
    double TotalResourceFlowRatio = 0.0;

    std::optional<int> FifoIndex;

    TResourceVector GetGuaranteeShare() const;

    void SetFairShare(const TResourceVector& fairShare);
};

////////////////////////////////////////////////////////////////////////////////

//! Adjusts |proposedIntegralShare| so that the total guarantee share does not exceed limits share.
//! If |strongGuaranteeShare| + |proposedIntegralShare| <= |limitShare|, returns |proposedIntegralShare|.
//! Otherwise (due to a precision error), slightly decreases components of |proposedIntegralShare| until the inequality holds
//! and returns the resulting vector.
TResourceVector AdjustProposedIntegralShare(
    const TResourceVector& limitsShare,
    const TResourceVector& strongGuaranteeShare,
    TResourceVector proposedIntegralShare);

////////////////////////////////////////////////////////////////////////////////

class TElement
    : public virtual TRefCounted
{
public:
    virtual const TJobResources& GetResourceDemand() const = 0;
    virtual const TJobResources& GetResourceUsageAtUpdate() const = 0;
    // New method - should incapsulate ResourceLimits_ calculation logic and BestAllocation logic for operations.
    virtual const TJobResources& GetResourceLimits() const = 0;

    virtual const TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const = 0;
    virtual double GetWeight() const = 0;

    virtual TSchedulableAttributes& Attributes() = 0;
    virtual const TSchedulableAttributes& Attributes() const = 0;

    virtual TElement* GetParentElement() const = 0;

    virtual bool IsRoot() const;
    virtual bool IsOperation() const;
    TPool* AsPool();
    TOperationElement* AsOperation();

    virtual TString GetId() const = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;
    virtual bool AreDetailedLogsEnabled() const = 0;

    // It is public for testing purposes.
    void ResetFairShareFunctions();

private:
    bool AreFairShareFunctionsPrepared_ = false;
    std::optional<TVectorPiecewiseLinearFunction> FairShareByFitFactor_;
    std::optional<TVectorPiecewiseLinearFunction> FairShareBySuggestion_;
    std::optional<TScalarPiecewiseLinearFunction> MaxFitFactorBySuggestion_;

    TResourceVector TotalTruncatedFairShare_;

    virtual void PrepareFairShareFunctions(TFairShareUpdateContext* context);
    virtual void PrepareFairShareByFitFactor(TFairShareUpdateContext* context) = 0;
    void PrepareMaxFitFactorBySuggestion(TFairShareUpdateContext* context);

    virtual void DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context);
    virtual void UpdateCumulativeAttributes(TFairShareUpdateContext* context);
    virtual void ComputeAndSetFairShare(double suggestion, TFairShareUpdateContext* context) = 0;
    virtual void TruncateFairShareInFifoPools() = 0;

    void CheckFairShareFeasibility() const;

    virtual TResourceVector ComputeLimitsShare(const TFairShareUpdateContext* context) const;
    void UpdateAttributes(const TFairShareUpdateContext* context);

    TResourceVector GetVectorSuggestion(double suggestion) const;

    virtual void AdjustStrongGuarantees(const TFairShareUpdateContext* context);
    virtual void InitIntegralPoolLists(TFairShareUpdateContext* context);
    virtual void DistributeFreeVolume();

    TResourceVector GetTotalTruncatedFairShare() const;

    friend class TCompositeElement;
    friend class TPool;
    friend class TRootElement;
    friend class TOperationElement;
    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TElement)
DEFINE_REFCOUNTED_TYPE(TElement)

////////////////////////////////////////////////////////////////////////////////

class TCompositeElement
    : public virtual TElement
{
public:
    virtual TElement* GetChild(int index) = 0;
    virtual const TElement* GetChild(int index) const = 0;
    virtual int GetChildCount() const = 0;

    virtual ESchedulingMode GetMode() const = 0;
    virtual bool HasHigherPriorityInFifoMode(const TElement* lhs, const TElement* rhs) const = 0;

    virtual double GetSpecifiedBurstRatio() const = 0;
    virtual double GetSpecifiedResourceFlowRatio() const = 0;

    virtual bool IsFairShareTruncationInFifoPoolEnabled() const = 0;
    virtual bool CanAcceptFreeVolume() const = 0;
    virtual bool ShouldDistributeFreeVolumeAmongChildren() const = 0;

private:
    using TChildSuggestions = std::vector<double>;

    std::vector<TElement*> SortedChildren_;

    void PrepareFairShareFunctions(TFairShareUpdateContext* context) override;
    void PrepareFairShareByFitFactor(TFairShareUpdateContext* context) override;
    void PrepareFairShareByFitFactorFifo(TFairShareUpdateContext* context);
    void PrepareFairShareByFitFactorNormal(TFairShareUpdateContext* context);

    void AdjustStrongGuarantees(const TFairShareUpdateContext* context) override;
    void InitIntegralPoolLists(TFairShareUpdateContext* context) override;
    void DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context) override;
    void DetermineImplicitEffectiveStrongGuaranteeResources(
        const TJobResources& totalExplicitChildrenGuaranteeResources,
        TFairShareUpdateContext* context);
    void UpdateCumulativeAttributes(TFairShareUpdateContext* context) override;
    void UpdateOverflowAndAcceptableVolumesRecursively();
    void DistributeFreeVolume() override;
    void ComputeAndSetFairShare(double suggestion, TFairShareUpdateContext* context) override;
    void TruncateFairShareInFifoPools() override;

    void PrepareFifoPool();

    double GetMinChildWeight() const;

    /// strict_mode = true means that a caller guarantees that the sum predicate is true at least for fit factor = 0.0.
    /// strict_mode = false means that if the sum predicate is false for any fit factor, we fit children to the least possible sum
    /// (i. e. use fit factor = 0.0)
    template <class TValue, class TGetter, class TSetter>
    TValue ComputeByFitting(
        const TGetter& getter,
        const TSetter& setter,
        TValue maxSum,
        bool strictMode = true);

    TChildSuggestions GetChildSuggestionsFifo(double fitFactor);
    TChildSuggestions GetChildSuggestionsNormal(double fitFactor);

    friend class TPool;
    friend class TRootElement;
    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(CompositeElement)
DEFINE_REFCOUNTED_TYPE(TCompositeElement)

////////////////////////////////////////////////////////////////////////////////

class TPool
    : public virtual TCompositeElement
{
public:
    // NB: it is combination of options on pool and on tree.
    virtual TResourceVector GetIntegralShareLimitForRelaxedPool() const = 0;

    virtual const TIntegralResourcesState& IntegralResourcesState() const = 0;
    virtual TIntegralResourcesState& IntegralResourcesState() = 0;

    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const = 0;

private:
    void InitIntegralPoolLists(TFairShareUpdateContext* context) override;

    void UpdateAccumulatedResourceVolume(TFairShareUpdateContext* context);

    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TPool)
DEFINE_REFCOUNTED_TYPE(TPool)

////////////////////////////////////////////////////////////////////////////////

class TRootElement
    : public virtual TCompositeElement
{
public:
    bool IsRoot() const override;

private:
    void DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context) override;
    void UpdateCumulativeAttributes(TFairShareUpdateContext* context) override;
    void TruncateFairShareInFifoPools() override;

    void ValidateAndAdjustSpecifiedGuarantees(TFairShareUpdateContext* context);

    friend class TElement;
    friend class TCompositeElement;
    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TRootElement)
DEFINE_REFCOUNTED_TYPE(TRootElement)

////////////////////////////////////////////////////////////////////////////////

class TOperationElement
    : public virtual TElement
{
public:
    bool IsOperation() const override;

    virtual TResourceVector GetBestAllocationShare() const = 0;

    virtual bool IsGang() const = 0;

private:
    void PrepareFairShareByFitFactor(TFairShareUpdateContext* context) override;

    void ComputeAndSetFairShare(double suggestion, TFairShareUpdateContext* context) override;
    void TruncateFairShareInFifoPools() override;
    TResourceVector ComputeLimitsShare(const TFairShareUpdateContext* context) const override;

    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TOperationElement)
DEFINE_REFCOUNTED_TYPE(TOperationElement)

////////////////////////////////////////////////////////////////////////////////

struct TFairShareUpdateContext
{
    // TODO(eshcherbin): Create a separate fair share update config instead of passing all options in context.
    TFairShareUpdateContext(
        const TJobResources totalResourceLimits,
        const EJobResourceType mainResource,
        const TDuration integralPoolCapacitySaturationPeriod,
        const TDuration integralSmoothPeriod,
        const TInstant now,
        const std::optional<TInstant> previousUpdateTime);

    const TJobResources TotalResourceLimits;

    const EJobResourceType MainResource;
    const TDuration IntegralPoolCapacitySaturationPeriod;
    const TDuration IntegralSmoothPeriod;

    const TInstant Now;
    const std::optional<TInstant> PreviousUpdateTime;

    std::vector<TError> Errors;

    NProfiling::TCpuDuration PrepareFairShareByFitFactorTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorOperationsTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorFifoTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorNormalTotalTime = {};
    NProfiling::TCpuDuration PrepareMaxFitFactorBySuggestionTotalTime = {};
    NProfiling::TCpuDuration PointwiseMinTotalTime = {};
    NProfiling::TCpuDuration ComposeTotalTime = {};
    NProfiling::TCpuDuration CompressFunctionTotalTime = {};

    std::vector<TPool*> RelaxedPools;
    std::vector<TPool*> BurstPools;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateExecutor
{
public:
    TFairShareUpdateExecutor(
        const TRootElementPtr& rootElement,
        // TODO(ignat): split context on input and output parts.
        TFairShareUpdateContext* context);

    void Run();

private:
    const TRootElementPtr RootElement_;

    void ConsumeAndRefillIntegralPools();
    void UpdateBurstPoolIntegralShares();
    void UpdateRelaxedPoolIntegralShares();
    void UpdateRootFairShare();

    double GetIntegralShareRatioByVolume(const TPool* pool) const;
    TResourceVector GetHierarchicalAvailableLimitsShare(const TElement* element) const;
    void IncreaseHierarchicalIntegralShare(TElement* element, const TResourceVector& delta);

    TFairShareUpdateContext* Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

