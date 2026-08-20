#include "kqp_wasm_resident_string.h"

#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/wasm_string.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

//! Materialize a loop-invariant string argument into the query compartment's
//! linear memory exactly once per task and reuse the resulting resident value
//! for every per-row UDF call. The peephole optimizer only wraps subtrees that
//! do not depend on lambda arguments, so the value is stable for the whole task
//! and caching it is safe.
class TKqpWasmResidentStringWrapper : public TMutableComputationNode<TKqpWasmResidentStringWrapper> {
    using TBaseComputation = TMutableComputationNode<TKqpWasmResidentStringWrapper>;

    struct TCached : public TComputationValue<TCached> {
        using TComputationValue::TComputationValue;

        NUdf::TUnboxedValue Value;
    };

public:
    TKqpWasmResidentStringWrapper(TComputationMutables& mutables, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg(arg)
        , CachedIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto& cached = ctx.MutableValues[CachedIndex];
        if (cached.IsInvalid()) {
            cached = ctx.HolderFactory.Create<TCached>();
            auto& slot = *static_cast<TCached*>(cached.AsBoxed().Get());

            auto value = Arg->GetValue(ctx);
            if (!value.HasValue()) {
                // A null / empty argument: nothing to pin, keep it as is.
                slot.Value = std::move(value);
            } else {
                const NUdf::TStringRef ref = value.AsStringRef();
                slot.Value = NUdfStore::NWasm::TWasmStringValue::MakePreferWasm(ref);
                if (auto* handle = NUdfStore::NWasm::GetCurrentQueryCompartment()) {
                    handle->PreferWasm.OnResidentConstArg();
                }
            }
        }

        auto& slot = *static_cast<TCached*>(cached.AsBoxed().Get());
        return slot.Value;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg);
    }

    IComputationNode* const Arg;
    const ui32 CachedIndex;
};

} // namespace

IComputationNode* WrapKqpWasmResidentString(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "KqpWasmResidentString requires exactly 1 argument");

    auto arg = LocateNode(ctx.NodeLocator, callable, 0);
    return new TKqpWasmResidentStringWrapper(ctx.Mutables, arg);
}

} // namespace NMiniKQL
} // namespace NKikimr
