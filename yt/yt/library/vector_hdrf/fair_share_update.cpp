#include "fair_share_update.h"
#include "resource_helpers.h"
#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

// TODO(ignat): move finally to library
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/library/numeric/binary_search.h>

#include <yt/yt/library/vector_hdrf/piecewise_linear_function_helpers.h>

namespace NYT::NVectorHdrf {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TDetailedFairShare& detailedFairShare)
{
    return ToStringViaBuilder(detailedFairShare);
}

void FormatValue(TStringBuilderBase* builder, const TDetailedFairShare& detailedFairShare, TStringBuf /* format */)
{
    builder->AppendFormat(
        "{StrongGuarantee: %.6g, IntegralGuarantee: %.6g, WeightProportional: %.6g}",
        detailedFairShare.StrongGuarantee,
        detailedFairShare.IntegralGuarantee,
        detailedFairShare.WeightProportional);
}

////////////////////////////////////////////////////////////////////////////////

TResourceVector TSchedulableAttributes::GetGuaranteeShare() const
{
    return StrongGuaranteeShare + ProposedIntegralShare;
}

void TSchedulableAttributes::SetFairShare(const TResourceVector& fairShare)
{
    FairShare.Total = fairShare;
    FairShare.StrongGuarantee = TResourceVector::Min(fairShare, StrongGuaranteeShare);
    FairShare.IntegralGuarantee = TResourceVector::Min(fairShare - FairShare.StrongGuarantee, ProposedIntegralShare);
    FairShare.WeightProportional = fairShare - FairShare.StrongGuarantee - FairShare.IntegralGuarantee;
}

////////////////////////////////////////////////////////////////////////////////

TResourceVector AdjustProposedIntegralShare(
    const TResourceVector& limitsShare,
    const TResourceVector& strongGuaranteeShare,
    TResourceVector proposedIntegralShare)
{
    auto guaranteeShare = strongGuaranteeShare + proposedIntegralShare;
    if (!Dominates(limitsShare, guaranteeShare)) {
        YT_VERIFY(Dominates(limitsShare + TResourceVector::SmallEpsilon(), guaranteeShare));
        YT_VERIFY(Dominates(limitsShare, strongGuaranteeShare));

        proposedIntegralShare = limitsShare - strongGuaranteeShare;
        for (auto resource : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            constexpr int MaxAdjustmentIterationCount = 32;

            // NB(eshcherbin): Always should be no more than a single iteration, but to remove my paranoia I've bounded iteration count.
            int iterationCount = 0;
            while (limitsShare[resource] < strongGuaranteeShare[resource] + proposedIntegralShare[resource] &&
                iterationCount < MaxAdjustmentIterationCount)
            {
                proposedIntegralShare[resource] = std::nextafter(proposedIntegralShare[resource], 0.0);
                ++iterationCount;
            }
        }
    }

    return proposedIntegralShare;
}

////////////////////////////////////////////////////////////////////////////////

void TElement::DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* /* context */)
{ }

bool TElement::IsOperation() const
{
    return false;
}

bool TElement::IsRoot() const
{
    return false;
}

TPool* TElement::AsPool()
{
    return dynamic_cast<TPool*>(this);
}

TOperationElement* TElement::AsOperation()
{
    return dynamic_cast<TOperationElement*>(this);
}

void TElement::AdjustStrongGuarantees(const TFairShareUpdateContext* /* context */)
{ }

void TElement::InitIntegralPoolLists(TFairShareUpdateContext* /* context */)
{ }

void TElement::UpdateAttributes(const TFairShareUpdateContext* context)
{
    Attributes().LimitsShare = ComputeLimitsShare(context);
    YT_VERIFY(Dominates(TResourceVector::Ones(), Attributes().LimitsShare));
    YT_VERIFY(Dominates(Attributes().LimitsShare, TResourceVector::Zero()));

    Attributes().StrongGuaranteeShare = TResourceVector::FromJobResources(Attributes().EffectiveStrongGuaranteeResources, context->TotalResourceLimits);

    // NB: We need to ensure that |FairShareByFitFactor_(0.0)| is less than or equal to |LimitsShare| so that there exists a feasible fit factor and |MaxFitFactorBySuggestion_| is well defined.
    // To achieve this we limit |StrongGuarantee| with |LimitsShare| here, and later adjust the sum of children's |StrongGuarantee| to fit into the parent's |StrongGuarantee|.
    // This way children can't ask more than parent's |LimitsShare| when given a zero suggestion.
    Attributes().StrongGuaranteeShare = TResourceVector::Min(Attributes().StrongGuaranteeShare, Attributes().LimitsShare);

    if (GetResourceUsageAtUpdate() == TJobResources()) {
        Attributes().DominantResource = GetDominantResource(GetResourceDemand(), context->TotalResourceLimits);
    } else {
        Attributes().DominantResource = GetDominantResource(GetResourceUsageAtUpdate(), context->TotalResourceLimits);
    }

    Attributes().UsageShare = TResourceVector::FromJobResources(GetResourceUsageAtUpdate(), context->TotalResourceLimits);
    Attributes().DemandShare = TResourceVector::FromJobResources(GetResourceDemand(), context->TotalResourceLimits);
    YT_VERIFY(Dominates(Attributes().DemandShare, Attributes().UsageShare));
}

void TElement::UpdateCumulativeAttributes(TFairShareUpdateContext* context)
{
    UpdateAttributes(context);
}

void TElement::CheckFairShareFeasibility() const
{
    const auto& demandShare = Attributes().DemandShare;
    const auto& fairShare = Attributes().FairShare.Total;
    bool isFairShareSignificantlyGreaterThanDemandShare =
        !Dominates(demandShare + TResourceVector::SmallEpsilon(), fairShare);
    if (isFairShareSignificantlyGreaterThanDemandShare) {
        std::vector<EJobResourceType> significantlyGreaterResources;
        for (auto resource : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            if (demandShare[resource] + RatioComputationPrecision <= fairShare[resource]) {
                significantlyGreaterResources.push_back(resource);
            }
        }

        const auto& Logger = GetLogger();
        YT_LOG_WARNING(
            "Fair share is significantly greater than demand share "
            "(FairShare: %v, DemandShare: %v, SignificantlyGreaterResources: %v)",
            fairShare,
            demandShare,
            significantlyGreaterResources);
    }
}

TResourceVector TElement::ComputeLimitsShare(const TFairShareUpdateContext* context) const
{
    return TResourceVector::FromJobResources(Min(GetResourceLimits(), context->TotalResourceLimits), context->TotalResourceLimits);
}

void TElement::ResetFairShareFunctions()
{
    AreFairShareFunctionsPrepared_ = false;
}

void TElement::PrepareFairShareFunctions(TFairShareUpdateContext* context)
{
    if (AreFairShareFunctionsPrepared_) {
        return;
    }

    {
        TWallTimer timer;
        PrepareFairShareByFitFactor(context);
        context->PrepareFairShareByFitFactorTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(FairShareByFitFactor_.has_value());
    NDetail::VerifyNondecreasing(*FairShareByFitFactor_, GetLogger());
    YT_VERIFY(FairShareByFitFactor_->IsTrimmed());

    {
        TWallTimer timer;
        PrepareMaxFitFactorBySuggestion(context);
        context->PrepareMaxFitFactorBySuggestionTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(MaxFitFactorBySuggestion_.has_value());
    YT_VERIFY(MaxFitFactorBySuggestion_->LeftFunctionBound() == 0.0);
    YT_VERIFY(MaxFitFactorBySuggestion_->RightFunctionBound() == 1.0);
    NDetail::VerifyNondecreasing(*MaxFitFactorBySuggestion_, GetLogger());
    YT_VERIFY(MaxFitFactorBySuggestion_->IsTrimmed());

    {
        TWallTimer timer;
        FairShareBySuggestion_ = FairShareByFitFactor_->Compose(*MaxFitFactorBySuggestion_);
        context->ComposeTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(FairShareBySuggestion_.has_value());
    YT_VERIFY(FairShareBySuggestion_->LeftFunctionBound() == 0.0);
    YT_VERIFY(FairShareBySuggestion_->RightFunctionBound() == 1.0);
    NDetail::VerifyNondecreasing(*FairShareBySuggestion_, GetLogger());
    YT_VERIFY(FairShareBySuggestion_->IsTrimmed());

    {
        TWallTimer timer;
        *FairShareBySuggestion_ = NDetail::CompressFunction(*FairShareBySuggestion_, NDetail::CompressFunctionEpsilon);
        context->CompressFunctionTotalTime += timer.GetElapsedCpuTime();
    }
    NDetail::VerifyNondecreasing(*FairShareBySuggestion_, GetLogger());

    AreFairShareFunctionsPrepared_ = true;
}

void TElement::PrepareMaxFitFactorBySuggestion(TFairShareUpdateContext* context)
{
    YT_VERIFY(FairShareByFitFactor_);

    std::vector<TScalarPiecewiseLinearFunction> mffForComponents;  // Mff stands for "MaxFitFactor".

    for (int r = 0; r < ResourceCount; r++) {
        // Fsbff stands for "FairShareByFitFactor".
        auto fsbffComponent = NDetail::ExtractComponent(r, *FairShareByFitFactor_);
        YT_VERIFY(fsbffComponent.IsTrimmed());

        double limit = Attributes().LimitsShare[r];
        // NB(eshcherbin): We definitely cannot use a precise inequality here. See YT-13864.
        YT_VERIFY(fsbffComponent.LeftFunctionValue() < limit + RatioComputationPrecision);
        limit = std::min(std::max(limit, fsbffComponent.LeftFunctionValue()), fsbffComponent.RightFunctionValue());

        double guarantee = Attributes().GetGuaranteeShare()[r];
        guarantee = std::min(std::max(guarantee, fsbffComponent.LeftFunctionValue()), limit);

        auto mffForComponent = std::move(fsbffComponent)
            .Transpose()
            .Narrow(guarantee, limit)
            .TrimLeft()
            .Shift(/* deltaArgument */ -guarantee)
            .ExtendRight(/* newRightBound */ 1.0)
            .Trim();
        mffForComponents.push_back(std::move(mffForComponent));
    }

    {
        TWallTimer timer;
        MaxFitFactorBySuggestion_ = PointwiseMin(mffForComponents);
        context->PointwiseMinTotalTime += timer.GetElapsedCpuTime();
    }
}

TResourceVector TElement::GetVectorSuggestion(double suggestion) const
{
    auto vectorSuggestion = TResourceVector::FromDouble(suggestion) + Attributes().StrongGuaranteeShare;
    vectorSuggestion = TResourceVector::Min(vectorSuggestion, Attributes().LimitsShare);
    return vectorSuggestion;
}

void TElement::DistributeFreeVolume()
{ }

TResourceVector TElement::GetTotalTruncatedFairShare() const
{
    return TotalTruncatedFairShare_;
}

////////////////////////////////////////////////////////////////////////////////

void TCompositeElement::DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context)
{
    TJobResources totalExplicitChildrenGuaranteeResources;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        auto* child = GetChild(childIndex);

        auto& childEffectiveGuaranteeResources = child->Attributes().EffectiveStrongGuaranteeResources;
        childEffectiveGuaranteeResources = ToJobResources(
            *child->GetStrongGuaranteeResourcesConfig(),
            /* defaultValue */ {});
        totalExplicitChildrenGuaranteeResources += childEffectiveGuaranteeResources;
    }

    const auto& effectiveStrongGuaranteeResources = Attributes().EffectiveStrongGuaranteeResources;
    if (!IsRoot() && !Dominates(effectiveStrongGuaranteeResources, totalExplicitChildrenGuaranteeResources)) {
        const auto& Logger = GetLogger();
        // NB: This should never happen because we validate the guarantees at master.
        YT_LOG_WARNING(
            "Total children's explicit strong guarantees exceeds the effective strong guarantee at pool"
            "(EffectiveStrongGuarantees: %v, TotalExplicitChildrenGuarantees: %v)",
            effectiveStrongGuaranteeResources,
            totalExplicitChildrenGuaranteeResources);
    }

    DetermineImplicitEffectiveStrongGuaranteeResources(totalExplicitChildrenGuaranteeResources, context);

    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        GetChild(childIndex)->DetermineEffectiveStrongGuaranteeResources(context);
    }
}

void TCompositeElement::DetermineImplicitEffectiveStrongGuaranteeResources(
    const TJobResources& totalExplicitChildrenGuaranteeResources,
    TFairShareUpdateContext* context)
{
    const auto& effectiveStrongGuaranteeResources = Attributes().EffectiveStrongGuaranteeResources;
    auto residualGuaranteeResources = Max(effectiveStrongGuaranteeResources - totalExplicitChildrenGuaranteeResources, TJobResources{});
    auto mainResourceType = context->MainResource;
    auto parentMainResourceGuarantee = GetResource(effectiveStrongGuaranteeResources, mainResourceType);
    auto doDetermineImplicitGuarantees = [&] (const auto TJobResourcesConfig::* resourceDataMember, EJobResourceType resourceType) {
        if (resourceType == mainResourceType) {
            return;
        }

        std::vector<std::optional<double>> implicitGuarantees;
        implicitGuarantees.resize(GetChildCount());

        auto residualGuarantee = GetResource(residualGuaranteeResources, resourceType);
        auto parentResourceGuarantee = GetResource(effectiveStrongGuaranteeResources, resourceType);
        double totalImplicitGuarantee = 0.0;
        for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
            auto* child = GetChild(childIndex);
            if (child->GetStrongGuaranteeResourcesConfig()->*resourceDataMember) {
                continue;
            }

            auto childMainResourceGuarantee = GetResource(child->Attributes().EffectiveStrongGuaranteeResources, mainResourceType);
            double mainResourceRatio = parentMainResourceGuarantee > 0
                ? childMainResourceGuarantee / parentMainResourceGuarantee
                : 0.0;

            auto& childImplicitGuarantee = implicitGuarantees[childIndex];
            childImplicitGuarantee = mainResourceRatio * parentResourceGuarantee;
            totalImplicitGuarantee += *childImplicitGuarantee;
        }

        // NB: It is possible to overcommit guarantees at the first level of the tree, so we don't want to do
        // additional checks and rescaling. Instead, we handle this later when we adjust |StrongGuaranteeShare|.
        if (!IsRoot() && totalImplicitGuarantee > residualGuarantee) {
            auto scalingFactor = residualGuarantee / totalImplicitGuarantee;
            for (auto& childImplicitGuarantee : implicitGuarantees) {
                if (childImplicitGuarantee) {
                    *childImplicitGuarantee *= scalingFactor;
                }
            }
        }

        for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
            auto* child = GetChild(childIndex);
            if (const auto& childImplicitGuarantee = implicitGuarantees[childIndex]) {
                SetResource(child->Attributes().EffectiveStrongGuaranteeResources, resourceType, *childImplicitGuarantee);
            }
        }
    };

    TJobResourcesConfig::ForEachResource(doDetermineImplicitGuarantees);
}

void TCompositeElement::InitIntegralPoolLists(TFairShareUpdateContext* context)
{
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        GetChild(childIndex)->InitIntegralPoolLists(context);
    }
}

void TCompositeElement::UpdateCumulativeAttributes(TFairShareUpdateContext* context)
{
    Attributes().BurstRatio = GetSpecifiedBurstRatio();
    Attributes().TotalBurstRatio = Attributes().BurstRatio;
    Attributes().ResourceFlowRatio = GetSpecifiedResourceFlowRatio();
    Attributes().TotalResourceFlowRatio = Attributes().ResourceFlowRatio;

    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        auto* child = GetChild(childIndex);
        child->UpdateCumulativeAttributes(context);

        Attributes().TotalResourceFlowRatio += child->Attributes().TotalResourceFlowRatio;
        Attributes().TotalBurstRatio += child->Attributes().TotalBurstRatio;
    }

    TElement::UpdateCumulativeAttributes(context);

    if (GetMode() == ESchedulingMode::Fifo) {
        PrepareFifoPool();
    }
}

void TCompositeElement::PrepareFifoPool()
{
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        YT_VERIFY(GetChild(childIndex)->IsOperation());
    }

    SortedChildren_.clear();
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        SortedChildren_.push_back(GetChild(childIndex));
    }

    std::sort(
        begin(SortedChildren_),
        end(SortedChildren_),
        std::bind(
            &TCompositeElement::HasHigherPriorityInFifoMode,
            this,
            std::placeholders::_1,
            std::placeholders::_2));

    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        SortedChildren_[childIndex]->Attributes().FifoIndex = childIndex;
    }
}

void TCompositeElement::AdjustStrongGuarantees(const TFairShareUpdateContext* context)
{
    const auto& Logger = GetLogger();

    TResourceVector totalPoolChildrenStrongGuaranteeShare;
    TResourceVector totalChildrenStrongGuaranteeShare;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = GetChild(childIndex);
        totalChildrenStrongGuaranteeShare += child->Attributes().StrongGuaranteeShare;

        if (!child->IsOperation()) {
            totalPoolChildrenStrongGuaranteeShare += child->Attributes().StrongGuaranteeShare;
        }
    }

    if (!Dominates(Attributes().StrongGuaranteeShare, totalPoolChildrenStrongGuaranteeShare)) {
        // Drop strong guarantee shares of operations, adjust strong guarantee shares of pools.
        for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
            auto* child = GetChild(childIndex);
            if (child->IsOperation()) {
                child->Attributes().StrongGuaranteeShare = TResourceVector::Zero();
            }
        }

        // Use binary search instead of division to avoid problems with precision.
        ComputeByFitting(
            /* getter */ [&] (double fitFactor, const TElement* child) -> TResourceVector {
                return child->Attributes().StrongGuaranteeShare * fitFactor;
            },
            /* setter */ [&] (TElement* child, const TResourceVector& value) {
                YT_LOG_DEBUG("Adjusting strong guarantee shares (ChildId: %v, OldStrongGuaranteeShare: %v, NewStrongGuaranteeShare: %v)",
                    child->GetId(),
                    child->Attributes().StrongGuaranteeShare,
                    value);
                child->Attributes().StrongGuaranteeShare = value;
            },
            /* maxSum */ Attributes().StrongGuaranteeShare);
    } else if (!Dominates(Attributes().StrongGuaranteeShare, totalChildrenStrongGuaranteeShare)) {
        // Adjust strong guarantee shares of operations, preserve strong guarantee shares of pools.
        ComputeByFitting(
            /* getter */ [&] (double fitFactor, const TElement* child) -> TResourceVector {
                if (child->IsOperation()) {
                    return child->Attributes().StrongGuaranteeShare * fitFactor;
                } else {
                    return child->Attributes().StrongGuaranteeShare;
                }
            },
            /* setter */ [&] (TElement* child, const TResourceVector& value) {
                YT_LOG_DEBUG("Adjusting string guarantee shares (ChildId: %v, OldStrongGuaranteeShare: %v, NewStrongGuaranteeShare: %v)",
                    child->GetId(),
                    child->Attributes().StrongGuaranteeShare,
                    value);
                child->Attributes().StrongGuaranteeShare = value;
            },
            /* maxSum */ Attributes().StrongGuaranteeShare);
    }

    if (IsRoot()) {
        Attributes().PromisedFairShare = TResourceVector::FromJobResources(context->TotalResourceLimits, context->TotalResourceLimits);
        Attributes().EstimatedGuaranteeShare = Attributes().StrongGuaranteeShare;
    }

    auto computeGuaranteeFairShare = [&] (TResourceVector TSchedulableAttributes::* estimatedGuaranteeFairShare) {
        double weightSum = 0.0;
        auto undistributedEstimatedGuaranteeFairShare = Attributes().*estimatedGuaranteeFairShare;
        for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
            auto* child = GetChild(childIndex);
            weightSum += child->GetWeight();

            // NB: Sum of total strong guarantee share and total resource flow can be greater than total resource limits. This results in a scheduler alert.
            // However, no additional adjustment is done so we need to handle this case here as well.
            child->Attributes().*estimatedGuaranteeFairShare = TResourceVector::Min(
                child->Attributes().StrongGuaranteeShare + TResourceVector::FromDouble(child->Attributes().TotalResourceFlowRatio),
                undistributedEstimatedGuaranteeFairShare);
            undistributedEstimatedGuaranteeFairShare -= child->Attributes().*estimatedGuaranteeFairShare;
        }

        for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
                auto* child = GetChild(childIndex);
                (child->Attributes().*estimatedGuaranteeFairShare)[resourceType] += undistributedEstimatedGuaranteeFairShare[resourceType] * child->GetWeight() / weightSum;
            }
        }
    };

    computeGuaranteeFairShare(/*estimatedGuaranteeFairShare*/ &TSchedulableAttributes::PromisedFairShare);
    computeGuaranteeFairShare(/*estimatedGuaranteeFairShare*/ &TSchedulableAttributes::EstimatedGuaranteeShare);

    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        GetChild(childIndex)->AdjustStrongGuarantees(context);
    }
}

template <class TValue, class TGetter, class TSetter>
TValue TCompositeElement::ComputeByFitting(
    const TGetter& getter,
    const TSetter& setter,
    TValue maxSum,
    bool strictMode)
{
    auto checkSum = [&] (double fitFactor) -> bool {
        TValue sum = {};
        for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
            const auto* child = GetChild(childIndex);
            sum += getter(fitFactor, child);
        }

        if constexpr (std::is_same_v<TValue, TResourceVector>) {
            return Dominates(maxSum, sum);
        } else {
            return maxSum >= sum;
        }
    };

    double fitFactor;
    if (!strictMode && !checkSum(0.0)) {
        // Even left bound doesn't satisfy predicate.
        fitFactor = 0.0;
    } else {
        // Run binary search to compute fit factor.
        fitFactor = FloatingPointInverseLowerBound(0.0, 1.0, checkSum);
    }

    TValue resultSum = {};

    // Compute actual values from fit factor.
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        auto* child = GetChild(childIndex);
        TValue value = getter(fitFactor, child);
        resultSum += value;
        setter(child, value);
    }

    return resultSum;
}

void TCompositeElement::PrepareFairShareFunctions(TFairShareUpdateContext* context)
{
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        auto* child = GetChild(childIndex);
        child->PrepareFairShareFunctions(context);
    }

    TElement::PrepareFairShareFunctions(context);
}

void TCompositeElement::PrepareFairShareByFitFactor(TFairShareUpdateContext* context)
{
    switch (GetMode()) {
        case ESchedulingMode::Fifo:
            PrepareFairShareByFitFactorFifo(context);
            break;

        case ESchedulingMode::FairShare:
            PrepareFairShareByFitFactorNormal(context);
            break;

        default:
            YT_ABORT();
    }
}

// Fit factor for a FIFO pool is defined as the number of satisfied children plus the suggestion
// of the first child that is not satisfied, if any.
// A child is said to be satisfied when it is suggested the whole cluster (|suggestion == 1.0|).
// Note that this doesn't necessarily mean that the child's demand is satisfied.
// For an empty FIFO pool fit factor is not well defined.
//
// The unambiguity of the definition of the fit factor follows the fact that the suggestion of
// an unsatisfied child is, by definition, less than 1.
//
// Note that we assume all children have no guaranteed resources, so for any child:
// |child->FairShareBySuggestion_(0.0) == TResourceVector::Zero()|, and 0.0 is not a discontinuity
// point of |child->FairShareBySuggestion_|.
void TCompositeElement::PrepareFairShareByFitFactorFifo(TFairShareUpdateContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorFifoTotalTime += timer.GetElapsedCpuTime();
    });

    if (GetChildCount() == 0) {
        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0.0, 1.0, TResourceVector::Zero());
        return;
    }

    double rightFunctionBound = GetChildCount();
    FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0.0, rightFunctionBound, TResourceVector::Zero());

    double currentRightBound = 0.0;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = SortedChildren_[childIndex];
        const auto& childFSBS = *child->FairShareBySuggestion_;

        // NB(eshcherbin): Children of FIFO pools don't have guaranteed resources. See the function comment.
        YT_VERIFY(childFSBS.IsTrimmedLeft() && childFSBS.IsTrimmedRight());
        YT_VERIFY(childFSBS.LeftFunctionValue() == TResourceVector::Zero());

        // TODO(antonkikh): This can be implemented much more efficiently by concatenating functions instead of adding.
        *FairShareByFitFactor_ += childFSBS
            .Shift(/* deltaArgument */ currentRightBound)
            .Extend(/* newLeftBound */ 0.0, /* newRightBound */ rightFunctionBound);
        currentRightBound += 1.0;
    }

    YT_VERIFY(currentRightBound == rightFunctionBound);
}

void TCompositeElement::PrepareFairShareByFitFactorNormal(TFairShareUpdateContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorNormalTotalTime += timer.GetElapsedCpuTime();
    });

    if (GetChildCount() == 0) {
        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0.0, 1.0, TResourceVector::Zero());
        return;
    }

    std::vector<TVectorPiecewiseLinearFunction> childrenFunctions;
    double minWeight = GetMinChildWeight();
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = GetChild(childIndex);
        const auto& childFSBS = *child->FairShareBySuggestion_;

        auto childFunction = childFSBS
            .ScaleArgument(child->GetWeight() / minWeight)
            .ExtendRight(/* newRightBound */ 1.0);

        childrenFunctions.push_back(std::move(childFunction));
    }

    FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Sum(childrenFunctions);
}

double TCompositeElement::GetMinChildWeight() const
{
    double minWeight = std::numeric_limits<double>::max();
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = GetChild(childIndex);
        if (child->GetWeight() > RatioComputationPrecision) {
            minWeight = std::min(minWeight, child->GetWeight());
        }
    }
    return minWeight;
}

// Returns a vector of suggestions for children from |SortedEnabledChildren_| based on the given fit factor.
TCompositeElement::TChildSuggestions TCompositeElement::GetChildSuggestionsFifo(double fitFactor)
{
    YT_VERIFY(fitFactor <= SortedChildren_.size());

    int satisfiedChildCount = static_cast<int>(fitFactor);
    double unsatisfiedChildSuggestion = fitFactor - satisfiedChildCount;

    TChildSuggestions childSuggestions(SortedChildren_.size(), 0.0);
    for (int i = 0; i < satisfiedChildCount; i++) {
        childSuggestions[i] = 1.0;
    }

    if (unsatisfiedChildSuggestion != 0.0) {
        childSuggestions[satisfiedChildCount] = unsatisfiedChildSuggestion;
    }

    return childSuggestions;
}

// Returns a vector of suggestions for children from |EnabledChildren_| based on the given fit factor.
TCompositeElement::TChildSuggestions TCompositeElement::GetChildSuggestionsNormal(double fitFactor)
{
    const double minWeight = GetMinChildWeight();

    TChildSuggestions childSuggestions;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = GetChild(childIndex);
        childSuggestions.push_back(std::min(1.0, fitFactor * (child->GetWeight() / minWeight)));
    }

    return childSuggestions;
}

void TCompositeElement::ComputeAndSetFairShare(double suggestion, TFairShareUpdateContext* context)
{
    const auto& Logger = GetLogger();

    if (GetChildCount() == 0) {
        Attributes().SetFairShare(TResourceVector::Zero());
        return;
    }

    auto suggestedFairShare = FairShareBySuggestion_->ValueAt(suggestion);

    // Find the right fit factor to use when computing suggestions for children.

    // NB(eshcherbin): Vector of suggestions returned by |getEnabledChildSuggestions| must be consistent
    // with |children|, i.e. i-th suggestion is meant to be given to i-th enabled child.
    // This implicit correspondence between children and suggestions is done for optimization purposes.
    std::vector<TElement*> children;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        children.push_back(
            GetMode() == ESchedulingMode::Fifo
                ? SortedChildren_[childIndex]
                : GetChild(childIndex));
    }

    auto getEnabledChildSuggestions = (GetMode() == ESchedulingMode::Fifo)
        ? std::bind(&TCompositeElement::GetChildSuggestionsFifo, this, std::placeholders::_1)
        : std::bind(&TCompositeElement::GetChildSuggestionsNormal, this, std::placeholders::_1);

    auto getChildrenSuggestedFairShare = [&] (double fitFactor) {
        auto childSuggestions = getEnabledChildSuggestions(fitFactor);
        YT_VERIFY(childSuggestions.size() == children.size());

        TResourceVector childrenSuggestedFairShare;
        for (int childIndex = 0; childIndex < std::ssize(children); ++childIndex) {
            const auto& child = children[childIndex];
            auto childSuggestion = childSuggestions[childIndex];
            childrenSuggestedFairShare += child->FairShareBySuggestion_->ValueAt(childSuggestion);
        }

        return childrenSuggestedFairShare;
    };
    auto checkFitFactor = [&] (double fitFactor) {
        // Check that we can safely use the given fit factor to compute suggestions for children.
        return Dominates(suggestedFairShare + TResourceVector::SmallEpsilon(), getChildrenSuggestedFairShare(fitFactor));
    };

    // Usually MFFBS(suggestion) is the right fit factor to use for child suggestions.
    auto fitFactor = MaxFitFactorBySuggestion_->ValueAt(suggestion);
    if (!checkFitFactor(fitFactor)) {
        YT_ASSERT(checkFitFactor(0.0));

        // However, sometimes we need to tweak MFFBS(suggestion) in order not to suggest too much to children.
        // NB(eshcherbin): Possible to optimize this by using galloping, as the target fit factor
        // should be very, very close to our first estimate.
        fitFactor = FloatingPointInverseLowerBound(
            /* lo */ 0.0,
            /* hi */ fitFactor,
            /* predicate */ checkFitFactor);
    }

    // Propagate suggestions to children and collect the total used fair share.

    auto childSuggestions = getEnabledChildSuggestions(fitFactor);
    YT_VERIFY(childSuggestions.size() == children.size());

    TResourceVector childrenUsedFairShare;
    for (int childIndex = 0; childIndex < std::ssize(children); ++childIndex) {
        const auto& child = children[childIndex];
        auto childSuggestion = childSuggestions[childIndex];
        child->ComputeAndSetFairShare(childSuggestion, context);
        childrenUsedFairShare += child->Attributes().FairShare.Total;
    }

    // Validate children total fair share.
    bool suggestedShareNearlyDominatesChildrenUsedShare =
        Dominates(suggestedFairShare + TResourceVector::SmallEpsilon(), childrenUsedFairShare);
    bool usedShareNearSuggestedShare =
        TResourceVector::Near(childrenUsedFairShare, suggestedFairShare, 1e-4 * MaxComponent(childrenUsedFairShare));

    YT_LOG_WARNING_UNLESS(usedShareNearSuggestedShare && suggestedShareNearlyDominatesChildrenUsedShare,
        "Fair share significantly differs from predicted in pool ("
        "Mode: %v, "
        "Suggestion: %.20v, "
        "VectorSuggestion: %.20v, "
        "SuggestedFairShare: %.20v, "
        "ChildrenUsedFairShare: %.20v, "
        "Difference: %.20v, "
        "FitFactor: %.20v, "
        "FSBFFPredicted: %.20v, "
        "ChildrenSuggestedFairShare: %.20v, "
        "ChildrenCount: %v)",
        GetMode(),
        suggestion,
        GetVectorSuggestion(suggestion),
        suggestedFairShare,
        childrenUsedFairShare,
        suggestedFairShare - childrenUsedFairShare,
        fitFactor,
        FairShareByFitFactor_->ValueAt(fitFactor),
        getChildrenSuggestedFairShare(fitFactor),
        GetChildCount());

    YT_VERIFY(suggestedShareNearlyDominatesChildrenUsedShare);

    // Set fair share.

    Attributes().SetFairShare(suggestedFairShare);
    CheckFairShareFeasibility();
}

void TCompositeElement::TruncateFairShareInFifoPools()
{
    THashSet<TElement*> truncatedChildren;
    if (GetMode() == ESchedulingMode::Fifo && IsFairShareTruncationInFifoPoolEnabled()) {
        for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
            auto *childOperation = SortedChildren_[childIndex]->AsOperation();

            YT_VERIFY(childOperation);

            const auto& childAttributes = childOperation->Attributes();
            auto childFairShare = childAttributes.FairShare.Total;
            if (childFairShare == TResourceVector::Zero()) {
                continue;
            }

            // NB(eshcherbin, YT-15061): This truncation is only used in GPU-trees to enable preemption of jobs of gang operations
            // which fair share is less than demand.
            bool isChildFullySatisfied = Dominates(childFairShare + TResourceVector::Epsilon(), childAttributes.DemandShare);
            bool shouldTruncate = !isChildFullySatisfied && childOperation->IsGang();
            if (shouldTruncate) {
                const auto& Logger = GetLogger();

                TotalTruncatedFairShare_ += childFairShare;
                childOperation->Attributes().SetFairShare(TResourceVector::Zero());
                truncatedChildren.insert(childOperation);

                YT_LOG_DEBUG("Truncated operation fair share in FIFO pool (OperationId: %v, TruncatedFairShare: %v)",
                    childOperation->GetId(),
                    childFairShare);
            }
        }
    }

    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        auto* child = GetChild(childIndex);
        if (!truncatedChildren.contains(child)) {
            child->TruncateFairShareInFifoPools();
            TotalTruncatedFairShare_ += child->GetTotalTruncatedFairShare();
        }
    }

    // TODO(eshcherbin): Should we use epsilon here?
    if (TotalTruncatedFairShare_ != TResourceVector::Zero()) {
        auto fairShare = TResourceVector::Max(Attributes().FairShare.Total - TotalTruncatedFairShare_, TResourceVector::Zero());
        Attributes().SetFairShare(fairShare);
    }
}

void TCompositeElement::UpdateOverflowAndAcceptableVolumesRecursively()
{
    const auto& Logger = GetLogger();
    auto& attributes = Attributes();

    auto thisPool = AsPool();
    if (thisPool && thisPool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None) {
        return;
    }

    TResourceVolume childrenAcceptableVolume;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        if (auto* childPool = GetChild(childIndex)->AsPool()) {
            childPool->UpdateOverflowAndAcceptableVolumesRecursively();
            attributes.ChildrenVolumeOverflow += childPool->Attributes().VolumeOverflow;
            childrenAcceptableVolume += childPool->Attributes().AcceptableVolume;
        }
    }

    bool canAcceptFreeVolume = CanAcceptFreeVolume();

    TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
        auto diff = attributes.ChildrenVolumeOverflow.*resourceDataMember - childrenAcceptableVolume.*resourceDataMember;
        if (diff > 0) {
            attributes.VolumeOverflow.*resourceDataMember = diff;
            attributes.AcceptableVolume.*resourceDataMember = 0;
        } else {
            attributes.VolumeOverflow.*resourceDataMember = 0;
            attributes.AcceptableVolume.*resourceDataMember = canAcceptFreeVolume ? -diff : 0;
        }
    });

    if (!attributes.VolumeOverflow.IsZero()) {
        YT_LOG_DEBUG("Pool has volume overflow (Volume: %v)", attributes.VolumeOverflow);
    }
}

void TCompositeElement::DistributeFreeVolume()
{
    const auto& Logger = GetLogger();
    auto& attributes = Attributes();

    TResourceVolume freeVolume = attributes.AcceptedFreeVolume;

    auto* thisPool = AsPool();
    if (thisPool && thisPool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None) {
        if (!freeVolume.IsZero()) {
            thisPool->IntegralResourcesState().AccumulatedVolume += freeVolume;
            YT_LOG_DEBUG("Pool has accepted free volume (FreeVolume: %v)", freeVolume);
        }
        return;
    }

    if (ShouldDistributeFreeVolumeAmongChildren() && !(freeVolume.IsZero() && attributes.ChildrenVolumeOverflow.IsZero())) {
        YT_LOG_DEBUG(
            "Distributing free volume among children (FreeVolumeFromParent: %v, ChildrenVolumeOverflow: %v)",
            freeVolume,
            attributes.ChildrenVolumeOverflow);

        freeVolume += attributes.ChildrenVolumeOverflow;

        struct TChildAttributes {
            int Index;
            double Weight;
            TSchedulableAttributes* Attributes;
            double AcceptableVolumeToWeightRatio;
        };

        TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
            if (freeVolume.*resourceDataMember  == 0) {
                return;
            }
            std::vector<TChildAttributes> hungryChildren;
            auto weightSum = 0.0;
            for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
                auto& childAttributes = GetChild(childIndex)->Attributes();
                if (childAttributes.AcceptableVolume.*resourceDataMember > RatioComputationPrecision &&
                    childAttributes.TotalResourceFlowRatio > RatioComputationPrecision)
                {
                    // Resource flow is taken as weight.
                    auto weight = childAttributes.TotalResourceFlowRatio;
                    hungryChildren.push_back(TChildAttributes{
                        .Index = childIndex,
                        .Weight = weight,
                        .Attributes = &childAttributes,
                        .AcceptableVolumeToWeightRatio = static_cast<double>(childAttributes.AcceptableVolume.*resourceDataMember) / weight,
                    });
                    weightSum += weight;
                }
            }

            // Children will be saturated in ascending order of |AcceptableVolumeToWeightRatio|.
            std::sort(
                hungryChildren.begin(),
                hungryChildren.end(),
                [] (const TChildAttributes& lhs, const TChildAttributes& rhs) {
                    return lhs.AcceptableVolumeToWeightRatio < rhs.AcceptableVolumeToWeightRatio;
                });

            auto it = hungryChildren.begin();
            // First we provide free volume to the pools that cannot fully consume the suggested volume.
            for (; it != hungryChildren.end(); ++it) {
                const auto suggestedFreeVolume = static_cast<double>(freeVolume.*resourceDataMember) * (it->Weight / weightSum);
                const auto acceptableVolume = it->Attributes->AcceptableVolume.*resourceDataMember;
                if (suggestedFreeVolume < acceptableVolume) {
                    break;
                }
                it->Attributes->AcceptedFreeVolume.*resourceDataMember = acceptableVolume;
                freeVolume.*resourceDataMember -= acceptableVolume;
                weightSum -= it->Weight;
            }

            // Then we provide free volume to remaining pools that will fully consume the suggested volume.
            for (; it != hungryChildren.end(); ++it) {
                auto suggestedFreeVolume = static_cast<double>(freeVolume.*resourceDataMember) * (it->Weight / weightSum);
                it->Attributes->AcceptedFreeVolume.*resourceDataMember = static_cast<std::remove_reference_t<decltype(freeVolume.*resourceDataMember)>>(suggestedFreeVolume);
            }
        });
    }

    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        GetChild(childIndex)->DistributeFreeVolume();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPool::InitIntegralPoolLists(TFairShareUpdateContext* context)
{
    switch (GetIntegralGuaranteeType()) {
        case EIntegralGuaranteeType::Burst:
            context->BurstPools.push_back(this);
            break;
        case EIntegralGuaranteeType::Relaxed:
            context->RelaxedPools.push_back(this);
            break;
        default:
            break;
    }
    TCompositeElement::InitIntegralPoolLists(context);
}

void TPool::UpdateAccumulatedResourceVolume(TFairShareUpdateContext* context)
{
    const auto& Logger = GetLogger();
    auto& attributes = Attributes();

    if (context->TotalResourceLimits == TJobResources()) {
        return;
    }

    if (!context->PreviousUpdateTime) {
        return;
    }

    auto periodSinceLastUpdate = context->Now - *context->PreviousUpdateTime;
    auto& integralResourcesState = IntegralResourcesState();

    auto oldVolume = integralResourcesState.AccumulatedVolume;
    auto poolCapacity = TResourceVolume(context->TotalResourceLimits * attributes.ResourceFlowRatio, context->IntegralPoolCapacitySaturationPeriod);

    auto zero = TResourceVolume();
    integralResourcesState.AccumulatedVolume +=
        TResourceVolume(context->TotalResourceLimits, periodSinceLastUpdate) * attributes.ResourceFlowRatio;
    integralResourcesState.AccumulatedVolume -=
        TResourceVolume(context->TotalResourceLimits, periodSinceLastUpdate) * integralResourcesState.LastShareRatio;
    integralResourcesState.AccumulatedVolume = Max(integralResourcesState.AccumulatedVolume, zero);

    auto upperLimit = Max(oldVolume, poolCapacity);

    attributes.VolumeOverflow = Max(integralResourcesState.AccumulatedVolume - upperLimit, TResourceVolume());
    if (CanAcceptFreeVolume()) {
        attributes.AcceptableVolume = Max(poolCapacity - integralResourcesState.AccumulatedVolume, zero);
    }

    integralResourcesState.AccumulatedVolume = Min(integralResourcesState.AccumulatedVolume, upperLimit);

    YT_LOG_DEBUG(
        "Accumulated resource volume updated "
        "(ResourceFlowRatio: %v, PeriodSinceLastUpdateInSeconds: %v, TotalResourceLimits: %v, LastIntegralShareRatio: %v, "
        "PoolCapacity: %v, OldVolume: %v, UpdatedVolume: %v, VolumeOverflow: %v, AcceptableVolume: %v)",
        attributes.ResourceFlowRatio,
        periodSinceLastUpdate.SecondsFloat(),
        context->TotalResourceLimits,
        integralResourcesState.LastShareRatio,
        poolCapacity,
        oldVolume,
        integralResourcesState.AccumulatedVolume,
        attributes.VolumeOverflow,
        attributes.AcceptableVolume);
}

////////////////////////////////////////////////////////////////////////////////

void TRootElement::DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context)
{
    Attributes().EffectiveStrongGuaranteeResources = context->TotalResourceLimits;

    TCompositeElement::DetermineEffectiveStrongGuaranteeResources(context);
}

bool TRootElement::IsRoot() const
{
    return true;
}

void TRootElement::UpdateCumulativeAttributes(TFairShareUpdateContext* context)
{
    TCompositeElement::UpdateCumulativeAttributes(context);

    Attributes().StrongGuaranteeShare = TResourceVector::Zero();
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = GetChild(childIndex);
        Attributes().StrongGuaranteeShare += child->Attributes().StrongGuaranteeShare;
    }
}

void TRootElement::TruncateFairShareInFifoPools()
{
    const auto& Logger = GetLogger();

    TCompositeElement::TruncateFairShareInFifoPools();

    YT_LOG_DEBUG_UNLESS(TotalTruncatedFairShare_ == TResourceVector::Zero(),
        "Truncated fair share in FIFO pools (NewFairShare: %v, TotalTruncatedFairShare: %v)",
        Attributes().FairShare.Total,
        TotalTruncatedFairShare_);
}

void TRootElement::ValidateAndAdjustSpecifiedGuarantees(TFairShareUpdateContext* context)
{
    auto totalResourceFlow = context->TotalResourceLimits * Attributes().TotalResourceFlowRatio;
    auto totalBurstResources = context->TotalResourceLimits * Attributes().TotalBurstRatio;
    TJobResources totalStrongGuaranteeResources;
    for (int childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        const auto* child = GetChild(childIndex);
        totalStrongGuaranteeResources += child->Attributes().EffectiveStrongGuaranteeResources;
    }

    if (!Dominates(context->TotalResourceLimits, totalStrongGuaranteeResources + totalResourceFlow)) {
        context->Errors.push_back(TError(EErrorCode::PoolTreeGuaranteesOvercommit, "Strong guarantees and resource flows exceed total cluster resources")
            << TErrorAttribute("total_strong_guarantee_resources", totalStrongGuaranteeResources)
            << TErrorAttribute("total_resource_flow", totalResourceFlow)
            << TErrorAttribute("total_cluster_resources", context->TotalResourceLimits));
    }

    if (!Dominates(context->TotalResourceLimits, totalStrongGuaranteeResources + totalBurstResources)) {
        context->Errors.push_back(TError(EErrorCode::PoolTreeGuaranteesOvercommit, "Strong guarantees and burst guarantees exceed total cluster resources")
            << TErrorAttribute("total_strong_guarantee_resources", totalStrongGuaranteeResources)
            << TErrorAttribute("total_burst_resources", totalBurstResources)
            << TErrorAttribute("total_cluster_resources", context->TotalResourceLimits));

        auto checkSum = [&] (double fitFactor) -> bool {
            auto sum = Attributes().StrongGuaranteeShare * fitFactor;
            for (const auto& pool : context->BurstPools) {
                sum += TResourceVector::FromDouble(pool->Attributes().BurstRatio) * fitFactor;
            }
            return Dominates(TResourceVector::Ones(), sum);
        };

        double fitFactor = FloatingPointInverseLowerBound(0.0, 1.0, checkSum);

        // NB(eshcherbin): Note that we validate the sum of EffectiveStrongGuaranteeResources but adjust StrongGuaranteeShare.
        // During validation we need to check the absolute values to handle corner cases correctly and always show the alert. See: YT-14758.
        // During adjustment we need to assure the invariants required for vector fair share computation.
        Attributes().StrongGuaranteeShare = Attributes().StrongGuaranteeShare * fitFactor;
        for (const auto& pool : context->BurstPools) {
            pool->Attributes().BurstRatio *= fitFactor;
        }
    }

    AdjustStrongGuarantees(context);
}

////////////////////////////////////////////////////////////////////////////////

bool TOperationElement::IsOperation() const
{
    return true;
}

void TOperationElement::PrepareFairShareByFitFactor(TFairShareUpdateContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorOperationsTotalTime += timer.GetElapsedCpuTime();
    });

    TVectorPiecewiseLinearFunction::TBuilder builder;

    // First we try to satisfy the current usage by giving equal fair share for each resource.
    // More precisely, for fit factor 0 <= f <= 1, fair share for resource r will be equal to min(usage[r], f * maxUsage).
    double maxUsage = MaxComponent(Attributes().UsageShare);
    if (maxUsage == 0.0) {
        builder.PushSegment({0.0, TResourceVector::Zero()}, {1.0, TResourceVector::Zero()});
    } else {
        TCompactVector<double, ResourceCount> sortedUsage(Attributes().UsageShare.begin(), Attributes().UsageShare.end());
        std::sort(sortedUsage.begin(), sortedUsage.end());

        builder.AddPoint({0.0, TResourceVector::Zero()});
        double previousUsageFitFactor = 0.0;
        for (auto usage : sortedUsage) {
            double currentUsageFitFactor = usage / maxUsage;
            if (currentUsageFitFactor > previousUsageFitFactor) {
                builder.AddPoint({
                    currentUsageFitFactor,
                    TResourceVector::Min(TResourceVector::FromDouble(usage), Attributes().UsageShare)});
                previousUsageFitFactor = currentUsageFitFactor;
            }
        }
        YT_VERIFY(previousUsageFitFactor == 1.0);
    }

    // After that we just give fair share proportionally to the remaining demand.
    builder.PushSegment({{1.0, Attributes().UsageShare}, {2.0, Attributes().DemandShare}});

    FairShareByFitFactor_ = builder.Finish();
}

void TOperationElement::ComputeAndSetFairShare(double suggestion, TFairShareUpdateContext* /*context*/)
{
    auto fairShare = FairShareBySuggestion_->ValueAt(suggestion);
    Attributes().SetFairShare(fairShare);
    CheckFairShareFeasibility();

    if (AreDetailedLogsEnabled()) {
        const auto& Logger = GetLogger();

        const auto fsbsSegment = FairShareBySuggestion_->SegmentAt(suggestion);
        const auto fitFactor = MaxFitFactorBySuggestion_->ValueAt(suggestion);
        const auto fsbffSegment = FairShareByFitFactor_->SegmentAt(fitFactor);

        YT_LOG_DEBUG(
            "Updated operation fair share ("
            "Suggestion: %.10g, "
            "UsedFairShare: %.10g, "
            "FSBSSegmentArguments: {%.10g, %.10g}, "
            "FSBSSegmentValues: {%.10g, %.10g}, "
            "FitFactor: %.10g, "
            "FSBFFSegmentArguments: {%.10g, %.10g}, "
            "FSBFFSegmentValues: {%.10g, %.10g})",
            suggestion,
            fairShare,
            fsbsSegment.LeftBound(), fsbsSegment.RightBound(),
            fsbsSegment.LeftValue(), fsbsSegment.RightValue(),
            fitFactor,
            fsbffSegment.LeftBound(), fsbffSegment.RightBound(),
            fsbffSegment.LeftValue(), fsbffSegment.RightValue());
    }
}

void TOperationElement::TruncateFairShareInFifoPools()
{ }

TResourceVector TOperationElement::ComputeLimitsShare(const TFairShareUpdateContext* context) const
{
    return TResourceVector::Min(TElement::ComputeLimitsShare(context), GetBestAllocationShare());
}

////////////////////////////////////////////////////////////////////////////////

TFairShareUpdateContext::TFairShareUpdateContext(
    const TJobResources totalResourceLimits,
    const EJobResourceType mainResource,
    const TDuration integralPoolCapacitySaturationPeriod,
    const TDuration integralSmoothPeriod,
    const TInstant now,
    const std::optional<TInstant> previousUpdateTime)
    : TotalResourceLimits(totalResourceLimits)
    , MainResource(mainResource)
    , IntegralPoolCapacitySaturationPeriod(integralPoolCapacitySaturationPeriod)
    , IntegralSmoothPeriod(integralSmoothPeriod)
    , Now(now)
    , PreviousUpdateTime(previousUpdateTime)
{ }

////////////////////////////////////////////////////////////////////////////////

TFairShareUpdateExecutor::TFairShareUpdateExecutor(
    const TRootElementPtr& rootElement,
    TFairShareUpdateContext* context)
    : RootElement_(rootElement)
    , Context_(context)
{ }

/// Steps of fair share update:
///
/// 1. Initialize burst and relaxed pool lists. This is a single pass through the tree.
///
/// 2. Update attributes needed for calculation of fair share (LimitsShare, DemandShare, UsageShare, StrongGuaranteeShare and others);
///
/// 3. Consume and refill accumulated resource volume of integral pools.
///   The amount of resources consumed by a pool is based on its integral guarantee share since the last fair share update.
///   Refilling is based on the resource flow ratio which was calculated in the previous step.
///
/// 4. Validate that the sum of burst and strong guarantee shares meet the total resources and that the strong guarantee share of every pool meets the limits share of that pool.
///   Shrink the guarantees in case of limits violations.
///
/// 5. Calculate integral shares for burst pools.
///   We temporarily increase the pool's resource guarantees by burst guarantees, and calculate how many resources the pool would consume within these extended guarantees.
///   Then we subtract the pool's strong guarantee share from the consumed resources to estimate the integral shares.
///   Descendants of burst pools have their fair share functions built on this step.
///
/// 6. Estimate the amount of available resources after satisfying strong and burst guarantees of all pools.
///
/// 7. Distribute available resources among the relaxed pools using binary search.
///   We build fair share functions for descendants of relaxed pools in this step.
///
/// 8. Build fair share functions and compute final fair shares of all pools.
///   The weight proportional component emerges here.
void TFairShareUpdateExecutor::Run()
{
    const auto& Logger = FairShareLogger;

    TWallTimer timer;

    RootElement_->DetermineEffectiveStrongGuaranteeResources(Context_);
    RootElement_->InitIntegralPoolLists(Context_);
    RootElement_->UpdateCumulativeAttributes(Context_);
    ConsumeAndRefillIntegralPools();
    RootElement_->ValidateAndAdjustSpecifiedGuarantees(Context_);

    UpdateBurstPoolIntegralShares();
    UpdateRelaxedPoolIntegralShares();

    RootElement_->PrepareFairShareFunctions(Context_);
    RootElement_->ComputeAndSetFairShare(/*suggestion*/ 1.0, Context_);
    RootElement_->TruncateFairShareInFifoPools();

    UpdateRootFairShare();

    auto totalDuration = timer.GetElapsedCpuTime();

    YT_LOG_DEBUG(
        "Finished updating fair share ("
        "TotalTime: %v, "
        "PrepareFairShareByFitFactor/TotalTime: %v, "
        "PrepareFairShareByFitFactor/Operations/TotalTime: %v, "
        "PrepareFairShareByFitFactor/Fifo/TotalTime: %v, "
        "PrepareFairShareByFitFactor/Normal/TotalTime: %v, "
        "PrepareMaxFitFactorBySuggestion/TotalTime: %v, "
        "PrepareMaxFitFactorBySuggestion/PointwiseMin/TotalTime: %v, "
        "Compose/TotalTime: %v., "
        "CompressFunction/TotalTime: %v)",
        CpuDurationToDuration(totalDuration).MicroSeconds(),
        CpuDurationToDuration(Context_->PrepareFairShareByFitFactorTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->PrepareFairShareByFitFactorOperationsTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->PrepareFairShareByFitFactorFifoTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->PrepareFairShareByFitFactorNormalTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->PrepareMaxFitFactorBySuggestionTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->PointwiseMinTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->ComposeTotalTime).MicroSeconds(),
        CpuDurationToDuration(Context_->CompressFunctionTotalTime).MicroSeconds());
}

void TFairShareUpdateExecutor::UpdateBurstPoolIntegralShares()
{
    const auto& Logger = FairShareLogger;

    for (auto& burstPool : Context_->BurstPools) {
        auto integralRatio = std::min(burstPool->Attributes().BurstRatio, GetIntegralShareRatioByVolume(burstPool));
        auto proposedIntegralShare = TResourceVector::Min(
            TResourceVector::FromDouble(integralRatio),
            GetHierarchicalAvailableLimitsShare(burstPool));
        YT_VERIFY(Dominates(proposedIntegralShare, TResourceVector::Zero()));

        proposedIntegralShare = AdjustProposedIntegralShare(
            burstPool->Attributes().LimitsShare,
            burstPool->Attributes().StrongGuaranteeShare,
            proposedIntegralShare);

        burstPool->Attributes().ProposedIntegralShare = proposedIntegralShare;
        burstPool->PrepareFairShareFunctions(Context_);
        burstPool->Attributes().ProposedIntegralShare = TResourceVector::Zero();

        auto fairShareWithinGuarantees = burstPool->FairShareBySuggestion_->ValueAt(0.0);
        auto integralShare = TResourceVector::Max(fairShareWithinGuarantees - burstPool->Attributes().StrongGuaranteeShare, TResourceVector::Zero());
        IncreaseHierarchicalIntegralShare(burstPool, integralShare);
        burstPool->ResetFairShareFunctions();
        burstPool->IntegralResourcesState().LastShareRatio = MaxComponent(integralShare);

        YT_LOG_DEBUG(
            "Provided integral share for burst pool "
            "(Pool: %v, ShareRatioByVolume: %v, ProposedIntegralShare: %v, FSWithingGuarantees: %v, IntegralShare: %v)",
            burstPool->GetId(),
            GetIntegralShareRatioByVolume(burstPool),
            proposedIntegralShare,
            fairShareWithinGuarantees,
            integralShare);
    }
}

void TFairShareUpdateExecutor::UpdateRelaxedPoolIntegralShares()
{
    const auto& Logger = FairShareLogger;

    if (Context_->RelaxedPools.empty()) {
        return;
    }

    auto availableShare = TResourceVector::Ones();
    for (int childIndex = 0; childIndex < RootElement_->GetChildCount(); ++childIndex) {
        const auto* child = RootElement_->GetChild(childIndex);
        auto usedShare = TResourceVector::Min(child->Attributes().GetGuaranteeShare(), child->Attributes().DemandShare);
        availableShare -= usedShare;
    }

    std::vector<TPool*> relaxedPools;
    std::vector<double> weights;
    std::vector<TResourceVector> originalLimits;
    for (auto& relaxedPool : Context_->RelaxedPools) {
        double integralShareRatio = GetIntegralShareRatioByVolume(relaxedPool);
        if (integralShareRatio == 0) {
            continue;
        }
        relaxedPools.push_back(relaxedPool);
        weights.push_back(integralShareRatio);
        originalLimits.push_back(relaxedPool->Attributes().LimitsShare);

        // It is incorporated version of this method below.
        // relaxedPool->ApplyLimitsForRelaxedPool();
        {
            auto relaxedPoolLimit = TResourceVector::Min(
                TResourceVector::FromDouble(integralShareRatio),
                relaxedPool->GetIntegralShareLimitForRelaxedPool());
            relaxedPoolLimit += relaxedPool->Attributes().StrongGuaranteeShare;
            relaxedPool->Attributes().LimitsShare = TResourceVector::Min(relaxedPool->Attributes().LimitsShare, relaxedPoolLimit);
        }

        relaxedPool->PrepareFairShareFunctions(Context_);
    }

    if (relaxedPools.empty()) {
        return;
    }

    double minWeight = *std::min_element(weights.begin(), weights.end());
    YT_VERIFY(minWeight > 0);
    for (auto& weight : weights) {
        weight = weight / minWeight;
    }

    auto checkFitFactor = [&] (double fitFactor) {
        TResourceVector fairShareResult;
        for (int index = 0; index < std::ssize(relaxedPools); ++index) {
            auto suggestion = std::min(1.0, fitFactor * weights[index]);
            auto fairShare = relaxedPools[index]->FairShareBySuggestion_->ValueAt(suggestion);
            fairShareResult += TResourceVector::Max(fairShare - relaxedPools[index]->Attributes().StrongGuaranteeShare, TResourceVector::Zero());
        }

        return Dominates(availableShare, fairShareResult);
    };

    auto fitFactor = FloatingPointInverseLowerBound(
        /* lo */ 0.0,
        /* hi */ 1.0,
        /* predicate */ checkFitFactor);

    for (int index = 0; index < std::ssize(relaxedPools); ++index) {
        auto weight = weights[index];
        const auto& relaxedPool = relaxedPools[index];
        auto suggestion = std::min(1.0, fitFactor * weight);
        auto fairShareWithinGuarantees = relaxedPool->FairShareBySuggestion_->ValueAt(suggestion);

        auto integralShare = TResourceVector::Max(fairShareWithinGuarantees - relaxedPool->Attributes().StrongGuaranteeShare, TResourceVector::Zero());

        relaxedPool->Attributes().LimitsShare = originalLimits[index];

        auto limitedIntegralShare = TResourceVector::Min(
            integralShare,
            GetHierarchicalAvailableLimitsShare(relaxedPool));
        YT_VERIFY(Dominates(limitedIntegralShare, TResourceVector::Zero()));
        IncreaseHierarchicalIntegralShare(relaxedPool, limitedIntegralShare);
        relaxedPool->ResetFairShareFunctions();
        relaxedPool->IntegralResourcesState().LastShareRatio = MaxComponent(limitedIntegralShare);

        YT_LOG_DEBUG("Provided integral share for relaxed pool "
            "(Pool: %v, ShareRatioByVolume: %v, Suggestion: %v, FSWithingGuarantees: %v, IntegralShare: %v, LimitedIntegralShare: %v)",
            relaxedPool->GetId(),
            GetIntegralShareRatioByVolume(relaxedPool),
            suggestion,
            fairShareWithinGuarantees,
            integralShare,
            limitedIntegralShare);
    }
}

void TFairShareUpdateExecutor::ConsumeAndRefillIntegralPools()
{
    for (auto* pool : Context_->BurstPools) {
        pool->UpdateAccumulatedResourceVolume(Context_);
    }
    for (auto* pool : Context_->RelaxedPools) {
        pool->UpdateAccumulatedResourceVolume(Context_);
    }

    RootElement_->UpdateOverflowAndAcceptableVolumesRecursively();
    RootElement_->DistributeFreeVolume();
}

void TFairShareUpdateExecutor::UpdateRootFairShare()
{
    // Make fair share at root equal to sum of children.
    TResourceVector totalUsedStrongGuaranteeShare;
    TResourceVector totalFairShare;
    for (int childIndex = 0; childIndex < RootElement_->GetChildCount(); ++childIndex) {
        const auto* child = RootElement_->GetChild(childIndex);
        totalUsedStrongGuaranteeShare += child->Attributes().FairShare.StrongGuarantee;
        totalFairShare += child->Attributes().FairShare.Total;
    }

    // NB(eshcherbin): In order to compute the detailed fair share components correctly,
    // we need to set |Attributes_.StrongGuaranteeShare| to the actual used strong guarantee share before calling |SetFairShare|.
    // However, afterwards it seems more natural to restore the previous value, which shows
    // the total configured strong guarantee shares in the tree.
    {
        auto staticStrongGuaranteeShare = RootElement_->Attributes().StrongGuaranteeShare;
        RootElement_->Attributes().StrongGuaranteeShare = totalUsedStrongGuaranteeShare;
        RootElement_->Attributes().SetFairShare(totalFairShare);
        RootElement_->Attributes().StrongGuaranteeShare = staticStrongGuaranteeShare;
    }
}

double TFairShareUpdateExecutor::GetIntegralShareRatioByVolume(const TPool* pool) const
{
    const auto& accumulatedVolume = pool->IntegralResourcesState().AccumulatedVolume;
    return accumulatedVolume.GetMinResourceRatio(Context_->TotalResourceLimits) /
        Context_->IntegralSmoothPeriod.SecondsFloat();
}

TResourceVector TFairShareUpdateExecutor::GetHierarchicalAvailableLimitsShare(const TElement* element) const
{
    auto* current = element;
    auto resultLimitsShare = TResourceVector::Ones();
    while (!current->IsRoot()) {
        const auto& limitsShare = current->Attributes().LimitsShare;
        const auto& effectiveGuaranteeShare = TResourceVector::Min(
            current->Attributes().GetGuaranteeShare(),
            current->Attributes().DemandShare);

        resultLimitsShare = TResourceVector::Min(resultLimitsShare, limitsShare - effectiveGuaranteeShare);
        YT_VERIFY(Dominates(resultLimitsShare, TResourceVector::Zero()));

        current = current->GetParentElement();
    }

    return resultLimitsShare;
}

void TFairShareUpdateExecutor::IncreaseHierarchicalIntegralShare(TElement* element, const TResourceVector& delta)
{
    auto* current = element;
    while (current) {
        // We allow guarantee share overcommit at root, because some part of strong guarantees can be reused as a relaxed integral share.
        auto increasedProposedIntegralShare = current->Attributes().ProposedIntegralShare + delta;
        if (!current->IsRoot()) {
            increasedProposedIntegralShare = AdjustProposedIntegralShare(
                current->Attributes().LimitsShare,
                current->Attributes().StrongGuaranteeShare,
                increasedProposedIntegralShare);
        }

        current->Attributes().ProposedIntegralShare = increasedProposedIntegralShare;
        current = current->GetParentElement();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
