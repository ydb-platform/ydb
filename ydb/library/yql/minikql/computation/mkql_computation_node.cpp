#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_impl.h"
#include "mkql_computation_node_pack.h"
#include "mkql_value_builder.h"
#include "mkql_validate.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

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

namespace NKikimr {
namespace NMiniKQL {

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
        return TArrowBlock::From(node->GetValue(ctx)).GetDatum();
    };
}

TComputationContext::TComputationContext(const THolderFactory& holderFactory,
    const NUdf::IValueBuilder* builder,
    const TComputationOptsFull& opts,
    const TComputationMutables& mutables,
    arrow::MemoryPool& arrowMemoryPool)
    : TComputationContextLLVM{holderFactory, opts.Stats, std::make_unique<NUdf::TUnboxedValue[]>(mutables.CurValueIndex), builder}
    , RandomProvider(opts.RandomProvider)
    , TimeProvider(opts.TimeProvider)
    , ArrowMemoryPool(arrowMemoryPool)
    , WideFields(mutables.CurWideFieldsIndex, nullptr)
    , TypeEnv(opts.TypeEnv)
    , Mutables(mutables)
    , TypeInfoHelper(new TTypeInfoHelper)
    , CountersProvider(opts.CountersProvider)
    , SecureParamsProvider(opts.SecureParamsProvider)
{
    std::fill_n(MutableValues.get(), mutables.CurValueIndex, NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::Invalid()));

    for (const auto& [mutableIdx, fieldIdx, used] : mutables.WideFieldInitialize) {
        for (ui32 i: used) {
            WideFields[fieldIdx + i] = &MutableValues[mutableIdx + i];
        }
    }
}

TComputationContext::~TComputationContext() {
#ifndef NDEBUG
    if (RssCounter) {
        Cerr << "UsageOnFinish: graph=" << HolderFactory.GetPagePool().GetUsed()
            << ", rss=" << TRusage::Get().MaxRss
            << ", peakAlloc=" << HolderFactory.GetPagePool().GetPeakAllocated()
            << ", adjustor=" << UsageAdjustor
            << Endl;
    }
#endif
}

void TComputationContext::UpdateUsageAdjustor(ui64 memLimit) {
    const auto rss = TRusage::Get().MaxRss;
    if (!InitRss) {
        LastRss = InitRss = rss;
    }

#ifndef NDEBUG
    // Print first time and then each 30 seconds
    bool printUsage = LastPrintUsage == TInstant::Zero()
        || TInstant::Now() > TDuration::Seconds(30).ToDeadLine(LastPrintUsage);
#endif

    if (auto peakAlloc = HolderFactory.GetPagePool().GetPeakAllocated()) {
        if (rss - InitRss > memLimit && rss - LastRss > (memLimit / 4)) {
            UsageAdjustor = std::max(1.f, float(rss - InitRss) / float(peakAlloc));
            LastRss = rss;
#ifndef NDEBUG
            printUsage = UsageAdjustor > 1.f;
#endif
        }
    }

#ifndef NDEBUG
    if (printUsage) {
        Cerr << "Usage: graph=" << HolderFactory.GetPagePool().GetUsed()
            << ", rss=" << rss
            << ", peakAlloc=" << HolderFactory.GetPagePool().GetPeakAllocated()
            << ", adjustor=" << UsageAdjustor
            << Endl;
        LastPrintUsage = TInstant::Now();
    }
#endif
}

class TSimpleSecureParamsProvider : public NUdf::ISecureParamsProvider {
public:
    TSimpleSecureParamsProvider(const THashMap<TString, TString>& secureParams)
        : SecureParams(secureParams)
    {}

    bool GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const override {
        auto found = SecureParams.FindPtr(TStringBuf(key));
        if (!found) {
            return false;
        }

        value = (TStringBuf)*found;
        return true;
    }

private:
    const THashMap<TString, TString> SecureParams;
};

std::unique_ptr<NUdf::ISecureParamsProvider> MakeSimpleSecureParamsProvider(const THashMap<TString, TString>& secureParams) {
    return std::make_unique<TSimpleSecureParamsProvider>(secureParams);
}

}
}
