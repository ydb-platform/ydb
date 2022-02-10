#include "yql_serializetypehandle.h"
#include "yql_type_resource.h"

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

class TSerializeTypeHandleWrapper : public TMutableComputationNode<TSerializeTypeHandleWrapper> {
    typedef TMutableComputationNode<TSerializeTypeHandleWrapper> TBaseComputation;
public:
    TSerializeTypeHandleWrapper(TComputationMutables& mutables, IComputationNode* handle)
        : TBaseComputation(mutables)
        , Handle_(handle)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto handle = Handle_->GetValue(ctx);
        auto type = GetYqlType(handle);
        return MakeString(NYql::NCommon::WriteTypeToYson(type));
    }

    void RegisterDependencies() const override {
        DependsOn(Handle_);
    }

private:
    IComputationNode* Handle_;
};

IComputationNode* WrapSerializeTypeHandle(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    Y_UNUSED(exprCtxMutableIndex);
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    auto handle = LocateNode(ctx.NodeLocator, callable, 0);
    return new TSerializeTypeHandleWrapper(ctx.Mutables, handle);
}

}
}
