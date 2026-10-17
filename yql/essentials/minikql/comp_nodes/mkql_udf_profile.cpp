#include "mkql_udf_profile.h"

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/hyperloglog/hyperloglog.h>

#include <util/datetime/base.h>
#include <util/digest/numeric.h>

namespace NKikimr::NMiniKQL {

namespace {

// Accumulates call statistics for a single UDF call site, for the lifetime
// of a TComputationContext. Reports 4 counters into the UDF counter
// infrastructure from its destructor, but only if the call site turned out
// to be "interesting" (i.e. at least one of the first GraceCount calls
// exceeded MinTime).
class TUdfProfileState {
public:
    enum class EMode {
        Unknown,
        Fast,
        Slow
    };

    TUdfProfileState(
        TString functionName,
        TDuration minTime,
        ui64 graceCount,
        ui32 hllPrecision,
        NUdf::ICountersProvider* provider)
        : FunctionName_(std::move(functionName))
        , MinTime_(minTime)
        , GraceCount_(graceCount)
        , Precision_(hllPrecision)
        , SizeLimit_((1U << hllPrecision) / 8)
        , Provider_(provider)
    {
    }

    ~TUdfProfileState() {
        if (Mode_ == EMode::Fast || !Provider_) {
            return;
        }

        const TStringBuf group("_UdfProfile");
        Provider_->GetCounter(group, FunctionName_ + "_CallCount", /*deriv=*/false).Set(CallCount_);
        Provider_->GetCounter(group, FunctionName_ + "_SlowCallCount", /*deriv=*/false).Set(SlowCallCount_);
        Provider_->GetCounter(group, FunctionName_ + "_Duration", /*deriv=*/false).Set(TotalTime_.MicroSeconds());
        Provider_->GetCounter(group, FunctionName_ + "_Cardinality", /*deriv=*/false).Set(Hll_ ? Hll_->Estimate() : Set_.size());
    }

    bool ShouldMeasure() const {
        return Mode_ != EMode::Fast;
    }

    void RecordCall(TDuration elapsed, ui64 argsHash) {
        // shuffle bits by multiply to the fibonacci constant, HLL uses mostly high bits
        // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        argsHash = 11400714819323198485LLU * argsHash;
        ++CallCount_;
        TotalTime_ += elapsed;
        if (elapsed >= MinTime_) {
            ++SlowCallCount_;
            Mode_ = EMode::Slow;
        }
        if (Hll_) {
            Hll_->Update(argsHash);
        } else {
            Set_.emplace(argsHash);
            if (Set_.size() > SizeLimit_) {
                Hll_ = THyperLogLogWithAlloc<TMKQLAllocator<ui8>>::Create(Precision_);
                for (auto h : Set_) {
                    Hll_->Update(h);
                }

                Set_.clear();
            }
        }
        if (Mode_ == EMode::Unknown && CallCount_ >= GraceCount_) {
            Mode_ = EMode::Fast;
        }
    }

private:
    const TString FunctionName_;
    const TDuration MinTime_;
    const ui64 GraceCount_;
    const ui32 Precision_;
    const ui64 SizeLimit_;
    NUdf::ICountersProvider* const Provider_;
    THashSet<ui64, std::hash<ui64>, std::equal_to<>, TMKQLAllocator<ui64>> Set_;
    TMaybe<THyperLogLogWithAlloc<TMKQLAllocator<ui8>>> Hll_;

    ui64 CallCount_ = 0;
    ui64 SlowCallCount_ = 0;
    TDuration TotalTime_;
    EMode Mode_ = EMode::Unknown;
};

// Thin box holding a TUdfProfileState by value, so it can live in a
// TComputationContext::MutableValues slot for the lifetime of the graph run,
// exactly like TUdfWrapper caches its resolved UDF implementation.
class TUdfProfileStateHolder: public NUdf::TBoxedValue {
public:
    template <typename... TArgs>
    explicit TUdfProfileStateHolder(TArgs&&... args)
        : State_(std::forward<TArgs>(args)...)
    {
    }

    TUdfProfileState& GetState() {
        return State_;
    }

private:
    TUdfProfileState State_;
};

// Wraps a UDF-produced callable value to time and count its invocations.
// If the wrapped call itself returns another callable (currying/closure),
// the intermediate call is not timed -- only the produced closure is
// re-wrapped, so the timing eventually attaches to the leaf callable that
// does not return a callable, as required by YQL-21019.
class TProfilingBoxedValue: public NUdf::TBoxedValue {
public:
    TProfilingBoxedValue(NUdf::TUnboxedValue&& inner, const TCallableType* funcType, TUdfProfileState* state)
        : Inner_(std::move(inner))
        , FuncType_(funcType)
        , State_(state)
    {
        if (!FuncType_->GetReturnType()->IsCallable()) {
            const auto argsCount = FuncType_->GetArgumentsCount();
            Hashers_.reserve(argsCount);
            for (ui32 i = 0; i < argsCount; ++i) {
                Hashers_.push_back(MakeHashImpl(FuncType_->GetArgumentType(i)));
            }
        }
    }

private:
    NUdf::TUnboxedValue Run(const NUdf::IValueBuilder* valueBuilder, const NUdf::TUnboxedValuePod* args) const final {
        const auto retType = FuncType_->GetReturnType();
        if (retType->IsCallable()) {
            auto result = Inner_.Run(valueBuilder, args);
            return NUdf::TUnboxedValuePod(new TProfilingBoxedValue(
                std::move(result), static_cast<TCallableType*>(retType), State_));
        }

        if (!State_->ShouldMeasure()) {
            return Inner_.Run(valueBuilder, args);
        }

        const auto start = Now();
        auto result = Inner_.Run(valueBuilder, args);
        const auto finish = Now();
        const auto elapsed = (finish >= start) ? (finish - start) : TDuration();

        ui64 combined = 0;
        for (ui32 i = 0; i < Hashers_.size(); ++i) {
            combined = CombineHashes(combined, Hashers_[i]->Hash(args[i]));
        }
        State_->RecordCall(elapsed, combined);
        return result;
    }

    const NUdf::TUnboxedValue Inner_;
    const TCallableType* const FuncType_;
    TUdfProfileState* const State_;
    TVector<NUdf::IHash::TPtr> Hashers_;
};

// Returns the leaf (non-Callable-returning) callable type reachable from
// `funcType` by following GetReturnType(), i.e. the type describing the
// actual invocation that does the work, as opposed to intermediate
// currying/closure levels.
const TCallableType* FindLeafFuncType(const TCallableType* funcType) {
    auto leaf = funcType;
    while (leaf->GetReturnType()->IsCallable()) {
        leaf = static_cast<TCallableType*>(leaf->GetReturnType());
    }
    return leaf;
}

bool IsLeafHashable(const TCallableType* leafType) {
    const auto argsCount = leafType->GetArgumentsCount();
    for (ui32 i = 0; i < argsCount; ++i) {
        if (!CanHash(leafType->GetArgumentType(i))) {
            return false;
        }
    }
    return true;
}

} // namespace

NUdf::TUnboxedValue MaybeWrapUdfProfiling(
    NUdf::TUnboxedValue value,
    const TCallableType* funcType,
    const TString& functionName,
    TComputationContext& ctx,
    ui32 profileStateIndex)
{
    if (!ctx.RuntimeSettings.UdfProfileEnable.Get()) {
        return value;
    }

    if (ctx.RuntimeSettings.UdfProfileExcludeModules.Get().contains(ModuleName(functionName))) {
        return value;
    }

    if (!IsLeafHashable(FindLeafFuncType(funcType))) {
        return value;
    }

    auto& stateSlot = ctx.MutableValues[profileStateIndex];
    if (!stateSlot.HasValue()) {
        stateSlot = NUdf::TUnboxedValuePod(new TUdfProfileStateHolder(
            functionName,
            ctx.RuntimeSettings.UdfProfileMinTimeUs.Get(),
            ctx.RuntimeSettings.UdfProfileGraceCount.Get(),
            ctx.RuntimeSettings.UdfProfileHLLPrecision.Get(),
            ctx.CountersProvider));
    }

    auto* holder = static_cast<TUdfProfileStateHolder*>(stateSlot.AsBoxed().Get());
    return NUdf::TUnboxedValuePod(new TProfilingBoxedValue(std::move(value), funcType, &holder->GetState()));
}

} // namespace NKikimr::NMiniKQL
