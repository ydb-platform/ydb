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

class TFuzzStreamWrapper: public TMutableComputationNode<TFuzzStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TFuzzStreamWrapper>;

public:
    TFuzzStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, ui64 fuzzId, const TFuzzerHolder& fuzzerHolder)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , FuzzId_(fuzzId)
        , FuzzerHolder_(fuzzerHolder)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Stream_->GetValue(ctx), FuzzId_, FuzzerHolder_);
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& stream,
                     ui64 fuzzId, const TFuzzerHolder& fuzzerHolder)
            : TBase(memInfo)
            , Ctx_(ctx)
            , Stream_(std::move(stream))
            , FuzzId_(fuzzId)
            , FuzzerHolder_(fuzzerHolder)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            NUdf::TUnboxedValue item;
            const auto status = Stream_.Fetch(item);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }
            result = FuzzerHolder_.ApplyFuzzers(item.Release(), FuzzId_, Ctx_.HolderFactory, Ctx_.ArrowMemoryPool, Ctx_.RandomProvider);
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& Ctx_;
        const NUdf::TUnboxedValue Stream_;
        const ui64 FuzzId_;
        const TFuzzerHolder& FuzzerHolder_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const ui64 FuzzId_;
    const TFuzzerHolder& FuzzerHolder_;
};

class TWideFuzzStreamWrapper: public TMutableComputationNode<TWideFuzzStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TWideFuzzStreamWrapper>;

public:
    TWideFuzzStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, TVector<ui64> fuzzIds, const TFuzzerHolder& fuzzerHolder)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , FuzzIds_(std::move(fuzzIds))
        , FuzzerHolder_(fuzzerHolder)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Stream_->GetValue(ctx), FuzzIds_, FuzzerHolder_);
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& stream,
                     const TVector<ui64>& fuzzIds, const TFuzzerHolder& fuzzerHolder)
            : TBase(memInfo)
            , Ctx_(ctx)
            , Stream_(std::move(stream))
            , FuzzIds_(fuzzIds)
            , FuzzerHolder_(fuzzerHolder)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) final {
            const auto status = Stream_.WideFetch(output, width);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }
            for (ui32 i = 0; i < width; ++i) {
                output[i] = FuzzerHolder_.ApplyFuzzers(output[i].Release(), FuzzIds_[i], Ctx_.HolderFactory, Ctx_.ArrowMemoryPool, Ctx_.RandomProvider);
            }
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& Ctx_;
        const NUdf::TUnboxedValue Stream_;
        const TVector<ui64> FuzzIds_;
        const TFuzzerHolder& FuzzerHolder_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const TVector<ui64> FuzzIds_;
    const TFuzzerHolder& FuzzerHolder_;
};

} // namespace

TRuntimeNode TBlockHelper::ConvertLiteralListToDatum(TRuntimeNode list, ui64 fuzzId) {
    auto blockStream = Pb_.FromFlow(Pb_.ToBlocks(Pb_.ToFlow(list)));
    auto fuzzedBlocks = FuzzStream(Pb_, blockStream, fuzzId);
    return MaterializeBlockStream(Pb_, fuzzedBlocks);
}

IComputationNode* TBlockHelper::WrapFuzzStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "Stream expected");
    auto streamType = AS_TYPE(TStreamType, callable.GetInput(0).GetStaticType());
    MKQL_ENSURE(streamType->GetItemType()->IsBlock(), "Block stream expected");
    const ui64 fuzzId = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui64>();
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    return new TFuzzStreamWrapper(ctx.Mutables, stream, fuzzId, FuzzerHolder_);
}

IComputationNode* TBlockHelper::WrapWideFuzzStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 arg");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsStream(), "Stream expected");
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    TVector<ui64> fuzzIds;
    fuzzIds.reserve(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        fuzzIds.push_back(AS_VALUE(TDataLiteral, callable.GetInput(i))->AsValue().Get<ui64>());
    }
    return new TWideFuzzStreamWrapper(ctx.Mutables, stream, std::move(fuzzIds), FuzzerHolder_);
}

TComputationNodeFactory TBlockHelper::GetNodeTestFactory() {
    return [this](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "FuzzStream") {
            return WrapFuzzStream(callable, ctx);
        }
        if (callable.GetType()->GetName() == "WideFuzzStream") {
            return WrapWideFuzzStream(callable, ctx);
        }
        if (auto* pgResult = NYql::GetPgFactory()(callable, ctx)) {
            return pgResult;
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode TBlockHelper::FuzzStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream, ui64 fuzzId) {
    MKQL_ENSURE(stream.GetStaticType()->IsStream(), "Stream expected");
    TCallableBuilder callableBuilder(pgmBuilder.GetTypeEnvironment(), "FuzzStream", stream.GetStaticType());
    callableBuilder.Add(stream);
    callableBuilder.Add(pgmBuilder.NewDataLiteral<ui64>(fuzzId));
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TBlockHelper::WideFuzzStream(TProgramBuilder& pgmBuilder, TRuntimeNode wideStream, const TVector<ui64>& fuzzIds) {
    MKQL_ENSURE(wideStream.GetStaticType()->IsStream(), "Stream expected");
    TCallableBuilder cb(pgmBuilder.GetTypeEnvironment(), "WideFuzzStream", wideStream.GetStaticType());
    cb.Add(wideStream);
    for (ui64 id : fuzzIds) {
        cb.Add(pgmBuilder.NewDataLiteral<ui64>(id));
    }
    return TRuntimeNode(cb.Build(), false);
}

TRuntimeNode TBlockHelper::MaterializeBlockStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    MKQL_ENSURE(stream.GetStaticType()->IsStream(), "Stream expected");
    auto streamType = AS_TYPE(TStreamType, stream.GetStaticType());
    MKQL_ENSURE(streamType->GetItemType()->IsBlock(), "Block stream expected");

    auto blocks = pgmBuilder.Collect(stream);
    auto message = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Utf8>("Block stream must have exactly one block.");
    auto exactlyOne = pgmBuilder.Equals(pgmBuilder.Length(blocks), pgmBuilder.NewDataLiteral<ui64>(1));
    auto single = pgmBuilder.Ensure(blocks, exactlyOne, message, __FILE__, __LINE__, 0);
    return pgmBuilder.Unwrap(pgmBuilder.Head(single), message, __FILE__, __LINE__, 0);
}

TVector<ui64> TBlockHelper::MakeWideStreamColumnsFuzzers(TMultiType* multiType) {
    const ui32 width = multiType->GetElementsCount();
    TVector<ui64> fuzzIds(width);
    for (ui32 i = 0; i + 1 < width; ++i) {
        fuzzIds[i] = FuzzerHolder_.ReserveFuzzer();
        FuzzerHolder_.CreateFuzzers(TFuzzOptions::FuzzAll(), fuzzIds[i], multiType->GetElementType(i), Pb_.GetTypeEnvironment(), Setup_.RuntimeSettings->DatumValidation.Get());
    }
    fuzzIds[width - 1] = TFuzzerHolder::EmptyFuzzerId;
    return fuzzIds;
}

TRuntimeNode TBlockHelper::BuildScalarOnlyWideBlockStream() {
    auto driver = Pb_.NewList(Pb_.NewDataType(NUdf::EDataSlot::Uint64), {Pb_.NewDataLiteral<ui64>(0)});
    auto length = Pb_.AsScalar(Pb_.NewDataLiteral<ui64>(1));
    auto wideFlow = Pb_.ExpandMap(Pb_.ToFlow(driver), [&, length](TRuntimeNode) -> TRuntimeNode::TList {
        return {length};
    });
    return Pb_.FromFlow(wideFlow);
}

TRuntimeNode TBlockHelper::BuildFuzzedWideStream(const TVector<TRuntimeNode>& vectorLists) {
    if (vectorLists.empty()) {
        return BuildScalarOnlyWideBlockStream();
    }
    const ui32 columns = vectorLists.size();
    auto zipped = Pb_.Zip(vectorLists);
    auto wideFlow = Pb_.ExpandMap(Pb_.ToFlow(zipped), [&](TRuntimeNode item) -> TRuntimeNode::TList {
        TRuntimeNode::TList result;
        for (ui32 i = 0; i < columns; ++i) {
            result.push_back(Pb_.Nth(item, i));
        }
        return result;
    });
    auto wideBlocks = Pb_.WideToBlocks(Pb_.FromFlow(wideFlow));
    auto multiType = AS_TYPE(TMultiType, AS_TYPE(TStreamType, wideBlocks.GetStaticType())->GetItemType());
    return WideFuzzStream(Pb_, wideBlocks, MakeWideStreamColumnsFuzzers(multiType));
}

TRuntimeNode TBlockHelper::ReadSingleWideStreamColumn(TRuntimeNode wideBlocks) {
    auto multiType = AS_TYPE(TMultiType, AS_TYPE(TStreamType, wideBlocks.GetStaticType())->GetItemType());
    MKQL_ENSURE(multiType->GetElementsCount() == 2,
                "Expected a single data column plus the trailing block-length scalar");
    auto expanded = Pb_.BlockExpandChunked(wideBlocks);
    auto narrow = Pb_.NarrowMap(Pb_.ToFlow(Pb_.WideFromBlocks(expanded)),
                                [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
    return Pb_.Collect(narrow);
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

void TBlockHelper::CompareDatums(arrow::Datum expected, arrow::Datum got) {
    auto typeToString = [](std::shared_ptr<arrow::DataType> type) -> TString {
        if (!type) {
            return "nullptr";
        }
        return type->ToString();
    };

    if (expected != got) {
        TStringBuilder message;
        message << "Datum comparison failed:\n";
        message << "Expected : " << DatumToString(expected) << "\n but got : " << DatumToString(got) << "\n";
        message << "Expected type : " << typeToString(expected.type()) << "\n but got type : " << typeToString(got.type()) << "\n";
        UNIT_FAIL(TString(message));
    }
}

TString TBlockHelper::TypeToString(TType* type) {
    auto typeHelper = TTypeInfoHelper();
    NYql::NUdf::TTypePrinter printer(typeHelper, type);
    TStringStream out;
    printer.Out(out);
    return out.Str();
}

arrow::Datum ConvertDatumToArrowFormat(arrow::Datum datum, arrow::MemoryPool& pool) {
    if (datum.is_scalar()) {
        return datum;
    }
    MKQL_ENSURE(datum.is_array(), "Chunked array is not supported yet.");
    auto result = datum.array()->Copy();
    if (result->type->id() == arrow::Type::STRUCT) {
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
