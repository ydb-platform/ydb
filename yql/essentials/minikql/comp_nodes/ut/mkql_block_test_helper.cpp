#include "mkql_block_test_helper.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
namespace NKikimr::NMiniKQL {

namespace {

class TMaterializeBlockStream: public TMutableComputationNode<TMaterializeBlockStream> {
    using TBaseComputation = TMutableComputationNode<TMaterializeBlockStream>;

public:
    TMaterializeBlockStream(TComputationMutables& mutables, IComputationNode* stream)
        : TBaseComputation(mutables)
        , Stream_(stream)
    {
    }
    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        NUdf::TUnboxedValue stream = Stream_->GetValue(ctx);

        NUdf::TUnboxedValue next;
        MKQL_ENSURE(stream.Fetch(next) == NYql::NUdf::EFetchStatus::Ok, "Stream must have at least 1 element.");
        NUdf::TUnboxedValue secondFetch;
        MKQL_ENSURE(stream.Fetch(secondFetch) == NYql::NUdf::EFetchStatus::Finish, "Stream must have exactly one element.");
        return next.Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* Stream_ = nullptr;
};

IComputationNode* WrapMaterializeBlockStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "Stream expected");
    auto streamType = AS_TYPE(TStreamType, callable.GetInput(0).GetStaticType());
    MKQL_ENSURE(streamType->GetItemType()->IsBlock(), "Block stream expected");
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    return new TMaterializeBlockStream(ctx.Mutables, stream);
}
} // namespace

TRuntimeNode TBlockHelper::ConvertLiteralListToDatum(TRuntimeNode::TList nodes) {
    auto list = Pb_.NewList(nodes[0].GetStaticType(), std::move(nodes));
    auto flowList = Pb_.ToFlow(list);
    auto blocksStream = Pb_.FromFlow(Pb_.ToBlocks(flowList));
    auto block = MaterializeBlockStream(*Setup_.PgmBuilder, blocksStream);
    return block;
}

TComputationNodeFactory TBlockHelper::GetNodeTestFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "MaterializeBlockStream") {
            return WrapMaterializeBlockStream(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

// Stream<Datum<X>> -> Datum<X>
TRuntimeNode TBlockHelper::MaterializeBlockStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    MKQL_ENSURE(stream.GetStaticType()->IsStream(), "Stream expected");
    auto streamType = AS_TYPE(TStreamType, stream.GetStaticType());
    MKQL_ENSURE(streamType->GetItemType()->IsBlock(), "Block stream expected");

    TCallableBuilder callableBuilder(pgmBuilder.GetTypeEnvironment(), __func__, streamType->GetItemType());
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TString TBlockHelper::DatumToString(arrow::Datum datum) {
    if (datum.is_array()) {
        return datum.make_array()->ToString();
    } else if (datum.is_scalar()) {
        return datum.scalar()->ToString();
    } else if (datum.is_arraylike()) {
        return "Chunked array (Detailed printing is not supported yet).";
    } else {
        MKQL_ENSURE(0, "Dont know what to do with " << datum.ToString() << " datum type.");
    }
}
} // namespace NKikimr::NMiniKQL
