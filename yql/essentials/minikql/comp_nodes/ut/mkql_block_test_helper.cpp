#include "mkql_block_test_helper.h"

#include "mkql_block_fuzzer.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>

namespace NKikimr::NMiniKQL {

namespace {

class TMaterializeBlockStream: public TMutableComputationNode<TMaterializeBlockStream> {
    using TBaseComputation = TMutableComputationNode<TMaterializeBlockStream>;

public:
    TMaterializeBlockStream(TComputationMutables& mutables, IComputationNode* stream, TType* type, ui64 fuzzId, const TFuzzerHolder& fuzzerHolder)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , Type_(type)
        , FuzzId_(fuzzId)
        , FuzzerHolder_(fuzzerHolder)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        NUdf::TUnboxedValue stream = Stream_->GetValue(ctx);
        Y_UNUSED(Type_);

        NUdf::TUnboxedValue next;
        MKQL_ENSURE(stream.Fetch(next) == NYql::NUdf::EFetchStatus::Ok, "Stream must have at least 1 element.");
        NUdf::TUnboxedValue secondFetch;
        MKQL_ENSURE(stream.Fetch(secondFetch) == NYql::NUdf::EFetchStatus::Finish, "Stream must have exactly one element.");
        auto result = FuzzerHolder_.ApplyFuzzers(next.Release(), FuzzId_, ctx.HolderFactory, ctx.ArrowMemoryPool, ctx.RandomProvider);
        return result.Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* Stream_ = nullptr;
    TType* Type_;
    ui64 FuzzId_;
    const TFuzzerHolder& FuzzerHolder_;
};

} // namespace

TRuntimeNode TBlockHelper::ConvertLiteralListToDatum(TRuntimeNode::TList nodes, ui64 fuzzId) {
    auto list = Pb_.NewList(nodes[0].GetStaticType(), std::move(nodes));
    auto flowList = Pb_.ToFlow(list);
    auto blocksStream = Pb_.FromFlow(Pb_.ToBlocks(flowList));
    auto block = MaterializeBlockStream(*Setup_.PgmBuilder, blocksStream, fuzzId);
    return block;
}

IComputationNode* TBlockHelper::WrapMaterializeBlockStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 1 arg");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "Stream expected");
    auto streamType = AS_TYPE(TStreamType, callable.GetInput(0).GetStaticType());
    MKQL_ENSURE(streamType->GetItemType()->IsBlock(), "Block stream expected");
    auto* underlyingType = AS_TYPE(TBlockType, streamType->GetItemType())->GetItemType();
    const ui32 fuzzId = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui64>();
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    return new TMaterializeBlockStream(ctx.Mutables, stream, underlyingType, fuzzId, FuzzerHolder_);
}

TComputationNodeFactory TBlockHelper::GetNodeTestFactory() {
    return [this](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "MaterializeBlockStream") {
            return WrapMaterializeBlockStream(callable, ctx);
        }
        if (auto* pgResult = NYql::GetPgFactory()(callable, ctx)) {
            return pgResult;
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

// Stream<Datum<X>> -> Datum<X>
TRuntimeNode TBlockHelper::MaterializeBlockStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream, ui64 fuzzId) {
    MKQL_ENSURE(stream.GetStaticType()->IsStream(), "Stream expected");
    auto streamType = AS_TYPE(TStreamType, stream.GetStaticType());
    MKQL_ENSURE(streamType->GetItemType()->IsBlock(), "Block stream expected");

    TCallableBuilder callableBuilder(pgmBuilder.GetTypeEnvironment(), __func__, streamType->GetItemType());
    callableBuilder.Add(stream);
    callableBuilder.Add(pgmBuilder.NewDataLiteral<ui64>(fuzzId));
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

arrow::Datum ConvertDatumToArrowFormat(arrow::Datum datum, arrow::MemoryPool& pool) {
    if (datum.is_scalar()) {
        return datum;
    }
    MKQL_ENSURE(datum.is_array(), "Chunked array is not supported yet.");
    auto result = datum.array()->Copy();
    if (result->type->id() == arrow::Type::STRUCT ||
        result->type->id() == arrow::Type::DENSE_UNION ||
        result->type->id() == arrow::Type::SPARSE_UNION) {
        if (result->buffers[0]) {
            result->buffers[0] = MakeDenseBitmapCopy(result->buffers[0]->data(), result->length, result->offset, &pool);
        }
        result->offset = 0;
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> children;
    for (const auto& child : result->child_data) {
        children.push_back(ConvertDatumToArrowFormat(*child, pool).array());
    }
    result->child_data = children;
    return result;
}

} // namespace NKikimr::NMiniKQL
