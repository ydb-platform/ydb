#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_impl.h"
#include "mkql_computation_node_pack.h"
#include "mkql_value_builder.h"
#include "mkql_validate.h"

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/generic/set.h>
#include <util/generic/algorithm.h>
#include <util/random/mersenne.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>
#include <util/system/fstat.h>
#include <util/system/rusage.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/memory/pool.h>

namespace NKikimr::NMiniKQL {

TComputationUpvalues::TComputationUpvalues(TComputationContext& ctx, IComputationNode* lambdaNode,
                                           const TComputationExternalNodePtrVector& argNodes) {
    std::set<const IComputationNode*> argSet(argNodes.cbegin(), argNodes.cend());
    for (const auto uv : lambdaNode->GetUpvalues()) {
        if (!argSet.contains(uv)) {
            UpvalueNodes_.push_back(uv);
        }
    }
    for (const auto uv : UpvalueNodes_) {
        ClosedUpvalues_.push_back(uv->GetValue(ctx));
    }
    PreservedUpvalues_.resize(ClosedUpvalues_.size());
}

void TComputationUpvalues::SetUpvalues(TComputationContext& ctx) const {
    for (size_t i = 0; i < UpvalueNodes_.size(); i++) {
        PreservedUpvalues_[i] = UpvalueNodes_[i]->GetValue(ctx);
        UpvalueNodes_[i]->SetValue(ctx, NUdf::TUnboxedValue(ClosedUpvalues_[i]));
    }
}

void TComputationUpvalues::RestoreUpvalues(TComputationContext& ctx) const {
    for (size_t i = 0; i < UpvalueNodes_.size(); i++) {
        UpvalueNodes_[i]->SetValue(ctx, std::move(PreservedUpvalues_[i]));
    }
}

std::unique_ptr<IArrowKernelComputationNode> IComputationNode::PrepareArrowKernelComputationNode(TComputationContext& ctx) const {
    Y_UNUSED(ctx);
    return {};
}

TDatumProvider MakeDatumProvider(const arrow::Datum& datum) {
    return [datum]() {
        return datum;
    };
}

TDatumProvider MakeDatumProvider(const IComputationNode* node, TComputationContext& ctx) {
    return [node, &ctx]() {
        const auto& value = node->GetValue(ctx);
        return TArrowBlock::From(value).GetDatum();
    };
}

NUdf::ITypeInfoHelper::TPtr TComputationContext::MakeTypeHelper(TMaybe<NUdf::TSourcePosition>& target) {
    auto ret = MakeIntrusive<TTypeInfoHelper>();
    ret->SetNotConsumedLinearCallback([&target](const NUdf::TSourcePosition& pos) {
        if (!target) {
            target = pos;
        }
    });

    return ret.Release();
}

TComputationContext::TComputationContext(const THolderFactory& holderFactory,
                                         const NUdf::IValueBuilder* builder,
                                         const TComputationOptsFull& opts,
                                         const TComputationMutables& mutables,
                                         arrow::MemoryPool& arrowMemoryPool,
                                         TMaybe<NUdf::TSourcePosition>& notConsumedLinear)
    : TComputationContextLLVM{holderFactory, opts.Stats, std::make_unique<NUdf::TUnboxedValue[]>(mutables.CurValueIndex), builder}
    , RandomProvider(opts.RandomProvider)
    , TimeProvider(opts.TimeProvider)
    , ArrowMemoryPool(arrowMemoryPool)
    , WideFields(mutables.CurWideFieldsIndex, nullptr)
    , TypeEnv(opts.TypeEnv)
    , Mutables(mutables)
    , TypeInfoHelper(MakeTypeHelper(notConsumedLinear))
    , CountersProvider(opts.CountersProvider)
    , SecureParamsProvider(opts.SecureParamsProvider)
    , LogProvider(opts.LogProvider)
    , LangVer(opts.LangVer)
    , NotConsumedLinear(notConsumedLinear)
{
    std::fill_n(MutableValues.get(), mutables.CurValueIndex, NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::Invalid()));

    for (const auto& [mutableIdx, fieldIdx, used] : mutables.WideFieldInitialize) {
        for (ui32 i : used) {
            WideFields[fieldIdx + i] = &MutableValues[mutableIdx + i];
        }
    }

    RssLogger_ = MakeLogger();
    RssLoggerComponent_ = RssLogger_->RegisterComponent("TrackRss");
}

TComputationContext::~TComputationContext() {
#ifndef NDEBUG
    if (RssCounter) {
        RssLogger_->Log(
            RssLoggerComponent_,
            NUdf::ELogLevel::Info,
            TStringBuilder() << "UsageOnFinish: graph=" << HolderFactory.GetPagePool().GetUsed()
                             << ", rss=" << TRusage::Get().MaxRss << ", peakAlloc="
                             << HolderFactory.GetPagePool().GetPeakAllocated() << ", adjustor=" << UsageAdjustor);
    }
#endif
}

NUdf::TLoggerPtr TComputationContext::MakeLogger() const {
    return LogProvider ? LogProvider->MakeLogger() : NUdf::MakeNullLogger();
}

void TComputationContext::UpdateUsageAdjustor(ui64 memLimit) {
    const auto rss = TRusage::Get().MaxRss;
    if (!InitRss_) {
        LastRss_ = InitRss_ = rss;
    }

#ifndef NDEBUG
    // Print first time and then each 30 seconds
    bool printUsage = LastPrintUsage_ == TInstant::Zero() ||
                      TInstant::Now() > TDuration::Seconds(30).ToDeadLine(LastPrintUsage_);
#endif

    if (auto peakAlloc = HolderFactory.GetPagePool().GetPeakAllocated()) {
        if (rss - InitRss_ > memLimit && rss - LastRss_ > (memLimit / 4)) {
            UsageAdjustor = std::max(1.f, float(rss - InitRss_) / float(peakAlloc));
            LastRss_ = rss;
#ifndef NDEBUG
            printUsage = UsageAdjustor > 1.f;
#endif
        }
    }

#ifndef NDEBUG
    if (printUsage) {
        RssLogger_->Log(
            RssLoggerComponent_,
            NUdf::ELogLevel::Info,
            TStringBuilder() << "Usage: graph=" << HolderFactory.GetPagePool().GetUsed()
                             << ", rss=" << rss << ", peakAlloc="
                             << HolderFactory.GetPagePool().GetPeakAllocated() << ", adjustor=" << UsageAdjustor);
        LastPrintUsage_ = TInstant::Now();
    }
#endif
}

class TSimpleSecureParamsProvider: public NUdf::ISecureParamsProvider {
public:
    explicit TSimpleSecureParamsProvider(const THashMap<TString, TString>& secureParams)
        : SecureParams_(secureParams)
    {
    }

    bool GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const override {
        auto found = SecureParams_.FindPtr(TStringBuf(key));
        if (!found) {
            return false;
        }

        value = (TStringBuf)*found;
        return true;
    }

private:
    const THashMap<TString, TString> SecureParams_;
};

std::unique_ptr<NUdf::ISecureParamsProvider> MakeSimpleSecureParamsProvider(const THashMap<TString, TString>& secureParams) {
    return std::make_unique<TSimpleSecureParamsProvider>(secureParams);
}

} // namespace NKikimr::NMiniKQL
