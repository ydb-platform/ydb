#include "mkql_next_value.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/utils/utf8.h>

#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;

namespace {

class TNextValueStringWrapper : public TMutableComputationNode<TNextValueStringWrapper> {
    using TSelf = TNextValueStringWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TNextValueStringWrapper(TComputationMutables& mutables, IComputationNode* source, EDataSlot slot)
        : TBaseComputation(mutables)
        , Source(source)
        , Slot(slot)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValue input = Source->GetValue(ctx);
        const auto& inputStr = input.AsStringRef();
        auto output = (Slot == EDataSlot::Utf8) ? NYql::NextValidUtf8(inputStr) : NYql::NextLexicographicString(inputStr);
        if (!output) {
            return {};
        }
        return MakeString(*output);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source);
    }

    IComputationNode* const Source;
    const EDataSlot Slot;
};

} // namespace

IComputationNode* WrapNextValue(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expecting exactly one argument");

    auto type = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(type->IsData(), "Expecting Data as argument");

    auto returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsOptional(), "Expecting Optional as return type");

    auto targetType = static_cast<TOptionalType*>(returnType)->GetItemType();
    MKQL_ENSURE(targetType->IsData(), "Expecting Optional of Data as return type");

    auto from = GetDataSlot(static_cast<TDataType*>(type)->GetSchemeType());
    auto to = GetDataSlot(static_cast<TDataType*>(targetType)->GetSchemeType());

    MKQL_ENSURE(from == to, "Input/output should have the same Data slot");

    MKQL_ENSURE(from == EDataSlot::String || from == EDataSlot::Utf8, "Only String or Utf8 is supported");

    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    return new TNextValueStringWrapper(ctx.Mutables, source, from);
}

}
}
