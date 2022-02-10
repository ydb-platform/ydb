#include "mkql_fromyson.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsOptional>
class TFromYsonSimpleTypeWrapper : public TMutableComputationNode<TFromYsonSimpleTypeWrapper<IsOptional>> {
    typedef TMutableComputationNode<TFromYsonSimpleTypeWrapper<IsOptional>> TBaseComputation;
public:
    TFromYsonSimpleTypeWrapper(TComputationMutables& mutables, IComputationNode* data, NUdf::TDataTypeId schemeType)
        : TBaseComputation(mutables)
        , Data(data)
        , SchemeType(NUdf::GetDataSlot(schemeType))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& data = Data->GetValue(ctx);
        if (IsOptional && !data) {
            return NUdf::TUnboxedValuePod();
        }

        return SimpleValueFromYson(SchemeType, data.AsStringRef());
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    IComputationNode* const Data;
    const NUdf::EDataSlot SchemeType;
};

}

IComputationNode* WrapFromYsonSimpleType(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    const auto dataSchemeType = dataType->GetSchemeType();
    MKQL_ENSURE(dataSchemeType == NUdf::TDataType<char*>::Id || dataSchemeType == NUdf::TDataType<NUdf::TYson>::Id,
        "Expected String or Yson");

    const auto schemeTypeData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const auto schemeType = schemeTypeData->AsValue().Get<ui32>();

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    if (isOptional) {
        return new TFromYsonSimpleTypeWrapper<true>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
    } else {
        return new TFromYsonSimpleTypeWrapper<false>(ctx.Mutables, data, static_cast<NUdf::TDataTypeId>(schemeType));
    }
}

}
}
